# main.py — drellia_envio (Cloud Function HTTP)
from __future__ import annotations

import json
import logging
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional

import functions_framework

import config
import customers_service
import db
import drellia_client
import messages_normalizer
# conversation_segmenter ya no se usa con el nuevo diseño

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)


# =============== HELPERS GENERALES ===============

def now_utc() -> datetime:
    return datetime.now(timezone.utc)


def now_ms() -> int:
    return int(now_utc().timestamp() * 1000)


# =============== AGREGACIÓN POR SESIÓN ===============

def aggregate_session_result(
    job: Dict[str, Any],
    segment_results: List[Dict[str, Any]],
) -> Dict[str, Any]:

    session_id = job["session_id"]
    lote_id = job.get("lote_id")

    if not segment_results:
        # no hubo segmentos (p.ej. solo mensajes customer sin bot/agent)
        return {
            "session_id": session_id,
            "lote_id": lote_id,
            "status": "SKIPPED",
            "reason": "NO_SEGMENTS",
            "http_code_conv": 0,
            "http_code_msgs": 0,
            "segments": [],
        }

    has_sent = any(r.get("status") == "SENT" for r in segment_results)
    has_failed = any(r.get("status") == "FAILED" for r in segment_results)

    if has_sent and not has_failed:
        status = "SENT"
    elif has_sent and has_failed:
        status = "PARTIAL"
    elif has_failed:
        status = "FAILED"
    else:
        status = "SKIPPED"

    http_conv_codes = [r.get("http_code_conv") or 0 for r in segment_results]
    http_msgs_codes = [r.get("http_code_msgs") or 0 for r in segment_results]

    http_code_conv = max(http_conv_codes) if http_conv_codes else 0
    http_code_msgs = max(http_msgs_codes) if http_msgs_codes else 0

    failed_reasons = list({
        (r.get("reason") or "")
        for r in segment_results
        if r.get("status") == "FAILED" and r.get("reason")
    })
    reason = "; ".join(failed_reasons) if failed_reasons else None

    return {
        "session_id": session_id,
        "lote_id": lote_id,
        "status": status,
        "reason": reason,
        "http_code_conv": http_code_conv,
        "http_code_msgs": http_code_msgs,
        "segments": segment_results,
    }


def update_envio_status_from_session_result(
    conn,
    session_result: Dict[str, Any],
) -> None:

    lote_id = session_result["lote_id"]
    session_id = session_result["session_id"]
    estado = session_result["status"]
    http_code_conv = session_result.get("http_code_conv") or 0
    http_code_msgs = session_result.get("http_code_msgs") or 0
    error_message = session_result.get("reason")

    db.update_envio_status(
        conn,
        lote_id=lote_id,
        session_id=session_id,
        estado=estado,
        http_code_conv=http_code_conv,
        http_code_msgs=http_code_msgs,
        error_message=error_message,
        sent_ts_ms=now_ms(),
    )


# =============== PROCESAMIENTO DE UN JOB ===============

def process_job(job: Dict[str, Any]) -> Dict[str, Any]:
    session_id = job["session_id"]
    lote_id = job.get("lote_id")

    conn = db.get_pg_conn()
    try:
        logger.info("[JOB][%s] Inicio procesamiento lote_id=%s", session_id, lote_id)

        # 1) CUSTOMER — Siempre recalcular y sincronizar
        customer_id = customers_service.ensure_customer_for_job(conn, job)

        if not customer_id:
            reason = "CUSTOMER_RESOLUTION_FAILED"
            logger.warning("[JOB][%s] %s", session_id, reason)
            session_result = {
                "session_id": session_id,
                "lote_id": lote_id,
                "status": "FAILED",
                "reason": reason,
                "http_code_conv": 0,
                "http_code_msgs": 0,
                "segments": [],
            }
            update_envio_status_from_session_result(conn, session_result)
            return session_result

        # 2) Normalizar mensajes de la sesión completa
        mensajes_raw = job.get("mensajes")
        normalized = messages_normalizer.normalize_messages(mensajes_raw)

        if not normalized:
            reason = "NO_VALID_MESSAGES"
            logger.info("[JOB][%s] SKIPPED: %s", session_id, reason)
            session_result = {
                "session_id": session_id,
                "lote_id": lote_id,
                "status": "SKIPPED",
                "reason": reason,
                "http_code_conv": 0,
                "http_code_msgs": 0,
                "segments": [],
            }
            update_envio_status_from_session_result(conn, session_result)
            return session_result

        # 3) Enviar la sesión completa a Drellia (1 conversación, múltiples empleados)
        session_result = drellia_client.send_session_to_drellia(
            conn=conn,
            job=job,
            normalized_messages=normalized,
            customer_id=customer_id,
        )

        # 4) Actualizar estado en envio_mensajes
        update_envio_status_from_session_result(conn, session_result)

        logger.info(
            "[JOB][%s] Fin procesamiento: status=%s",
            session_id, session_result["status"]
        )
        return session_result

    except Exception as e:
        logger.exception("[JOB][%s] EXCEPTION en procesamiento", session_id)
        # Intentar marcar FAILED genérico
        try:
            session_result = {
                "session_id": session_id,
                "lote_id": lote_id,
                "status": "FAILED",
                "reason": f"EXCEPTION: {e}",
                "http_code_conv": 0,
                "http_code_msgs": 0,
                "segments": [],
            }
            update_envio_status_from_session_result(conn, session_result)
            return session_result
        except Exception:
            # si hasta el update falla, devolvemos algo mínimo
            return {
                "session_id": session_id,
                "lote_id": lote_id,
                "status": "FAILED",
                "reason": f"EXCEPTION_NO_UPDATE: {e}",
                "http_code_conv": 0,
                "http_code_msgs": 0,
                "segments": [],
            }

    finally:
        try:
            conn.close()
        except Exception:
            pass


# =============== ENTRYPOINT HTTP ===============

@functions_framework.http
def drellia_envio(request):

    if request.method not in ("GET", "POST"):
        return ("ok", 200)

    try:
        # 1) Leer parámetros (query o body)
        args = request.args or {}
        body = {}
        try:
            if request.data:
                body = request.get_json(silent=True) or {}
        except Exception:
            body = {}

        lote_id = args.get("lote_id") or body.get("lote_id")
        limit_param = args.get("limit") or body.get("limit")
        limit = int(limit_param) if limit_param else None

        logger.info("[MAIN] Inicio drellia_envio lote_id=%s limit=%s", lote_id, str(limit))

        # 2) Cargar sesiones PENDING desde envio_mensajes
        conn = db.get_pg_conn()
        try:
            jobs = db.fetch_pending_from_envio(conn, lote_id, limit)
        finally:
            try:
                conn.close()
            except Exception:
                pass

        if not jobs:
            result = {
                "status": "NO_DATA",
                "message": "No hay conversaciones PENDING en envio_mensajes",
                "lote_id": lote_id,
            }
            return (json.dumps(result, default=str), 200, {"Content-Type": "application/json"})

        total_jobs = len(jobs)
        logger.info(
            "[MAIN] Procesando %d jobs en chunks de %d (MAX_WORKERS=%d)",
            total_jobs, config.CHUNK_SIZE, config.MAX_WORKERS
        )

        # 3) Procesar en chunks + paralelismo por sesión
        all_session_results: List[Dict[str, Any]] = []

        for start in range(0, total_jobs, config.CHUNK_SIZE):
            chunk = jobs[start:start + config.CHUNK_SIZE]
            logger.info(
                "[MAIN] Procesando chunk %d-%d (size=%d)",
                start, start + len(chunk) - 1, len(chunk)
            )

            chunk_results: List[Dict[str, Any]] = []
            max_workers = min(config.MAX_WORKERS, len(chunk))

            with ThreadPoolExecutor(max_workers=max_workers) as executor:
                future_to_job = {executor.submit(process_job, job): job for job in chunk}

                for fut in as_completed(future_to_job):
                    res = fut.result()
                    chunk_results.append(res)
                    all_session_results.append(res)

            # 4) Control simple de ratio de timeouts en el chunk
            timeout_errors = sum(
                1
                for r in chunk_results
                if r.get("status") == "FAILED"
                and r.get("reason")
                and ("HTTPSConnectionPool" in r["reason"] or "TIMEOUT" in r["reason"].upper())
            )
            timeout_ratio = timeout_errors / len(chunk) if len(chunk) > 0 else 0.0

            if timeout_ratio >= config.TIMEOUT_ERROR_THRESHOLD:
                logger.warning(
                    "[MAIN] Chunk %d-%d con timeout_ratio=%.2f (timeouts=%d/%d). Sleep 2s.",
                    start, start + len(chunk) - 1,
                    timeout_ratio, timeout_errors, len(chunk)
                )
                time.sleep(2)

        # 5) Resumen global de lote
        total_sessions = len(all_session_results)
        sent_ok = sum(1 for r in all_session_results if r["status"] == "SENT")
        sent_failed = sum(1 for r in all_session_results if r["status"] == "FAILED")
        sent_partial = sum(1 for r in all_session_results if r["status"] == "PARTIAL")
        skipped = sum(1 for r in all_session_results if r["status"] == "SKIPPED")

        summary_details = {
            "per_session": all_session_results,
        }

        # Para el resumen, si se pasó lote_id usamos ese, y lote_num del primer job
        lote_num = jobs[0].get("lote_num") if jobs else None
        summary_lote_id = lote_id if lote_id else None

        conn_sum = db.get_pg_conn()
        try:
            db.insert_lote_summary(
                conn_sum,
                lote_id=summary_lote_id,
                lote_num=lote_num,
                envio_ts=now_utc(),
                envio_ts_ms=now_ms(),
                total=total_sessions,
                sent_ok=sent_ok,
                sent_failed=sent_failed,
                details=summary_details,
            )
        finally:
            try:
                conn_sum.close()
            except Exception:
                pass

        result = {
            "status": "OK",
            "lote_id": summary_lote_id,
            "lote_num": lote_num,
            "total_sesiones": total_sessions,
            "enviadas_ok": sent_ok,
            "enviadas_error": sent_failed,
            "enviadas_parcial": sent_partial,
            "skipped": skipped,
            "sample_sessions": all_session_results[:5],
        }
        return (json.dumps(result, default=str), 200, {"Content-Type": "application/json"})

    except Exception as e:
        logger.exception("[MAIN] Error en drellia_envio")
        result = {"status": "ERROR", "message": str(e)}
        return (json.dumps(result, default=str), 500, {"Content-Type": "application/json"})