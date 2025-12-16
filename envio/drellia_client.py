# drellia_client.py
from __future__ import annotations

import json
import logging
from typing import Any, Dict, Optional, List

import requests

import config
import db
from models import ConversationSegment, NormalizedMessage

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

# ========= HTTP SESSION GLOBAL =========

_session = requests.Session()
_adapter = requests.adapters.HTTPAdapter(
    pool_connections=config.HTTP_POOL_CONNECTIONS,
    pool_maxsize=config.HTTP_POOL_MAXSIZE,
    max_retries=config.HTTP_MAX_RETRIES,
)
_session.mount("https://", _adapter)
_session.mount("http://", _adapter)

# ========= API KEY CACHE =========

_api_key_cache: Optional[str] = None


def get_drellia_api_key() -> str:

    global _api_key_cache
    if _api_key_cache:
        return _api_key_cache

    if config.DRELLIA_API_KEY_ENV and config.DRELLIA_API_KEY_ENV.strip():
        _api_key_cache = config.DRELLIA_API_KEY_ENV.strip()
        return _api_key_cache

    # Leemos del secreto
    key = db.get_secret_text(config.DRELLIA_SECRET_NAME).strip()
    _api_key_cache = key
    return key


# ========= HTTP HELPER CON RETRY =========

def _post_with_retry(
    url: str,
    headers: Dict[str, str],
    json_body: Any,
    timeout: int,
    max_retries: int = 2,
    retry_on_5xx: bool = True,
) -> requests.Response:
    last_exc: Optional[Exception] = None
    last_resp: Optional[requests.Response] = None

    for attempt in range(max_retries + 1):
        try:
            resp = _session.post(url, headers=headers, json=json_body, timeout=timeout)
            last_resp = resp

            if retry_on_5xx and 500 <= resp.status_code < 600 and attempt < max_retries:
                sleep_secs = 2 * (attempt + 1)
                logger.warning(
                    "[HTTP][RETRY] %s devolvió %s (5xx). intento=%d sleep=%ds",
                    url, resp.status_code, attempt + 1, sleep_secs,
                )
                import time
                time.sleep(sleep_secs)
                continue

            return resp

        except requests.Timeout as e:
            last_exc = e
            if attempt < max_retries:
                sleep_secs = 2 * (attempt + 1)
                logger.warning(
                    "[HTTP][RETRY] Timeout calling %s intento=%d sleep=%ds",
                    url, attempt + 1, sleep_secs
                )
                import time
                time.sleep(sleep_secs)
            else:
                logger.error(
                    "[HTTP] Timeout definitivo calling %s after %d attempts",
                    url, max_retries + 1
                )
                raise

    if last_resp is not None:
        return last_resp
    raise last_exc if last_exc else requests.Timeout("Unknown HTTP error")


# ========= RESOLVER EMPLOYEE POR SEGMENTO =========

def resolve_employee_id_for_segment(
    conn,
    job: Dict[str, Any],
    segment: ConversationSegment,
) -> Optional[str]:

    employee_type = segment.employee_type
    email = (segment.employee_email or "").strip().lower() if segment.employee_email else None

    # Caso BOT
    if employee_type == "BOT":
        if config.DRELLIA_BOT_EMPLOYEE_ID:
            return str(config.DRELLIA_BOT_EMPLOYEE_ID)

        # Fallback: usamos el agent_drellia_id si vino desde envio_mensajes
        fallback = job.get("agent_drellia_id")
        if fallback:
            logger.warning(
                "[EMPLOYEE_RESOLVE] BOT sin DRELLIA_BOT_EMPLOYEE_ID, usando fallback agent_drellia_id=%r",
                fallback
            )
            return str(fallback)

        logger.error(
            "[EMPLOYEE_RESOLVE] No se pudo resolver employeeId para BOT (no hay BOT_EMPLOYEE_ID ni fallback). job session=%s",
            job.get("session_id")
        )
        return None

    # Caso AGENT
    if employee_type == "AGENT":
        if email:
            drellia_uuid, _dept_id = db.get_agent_by_email(conn, email)
            if drellia_uuid:
                return str(drellia_uuid)
            else:
                logger.warning(
                    "[EMPLOYEE_RESOLVE] No se encontró agente con email=%s en tabla agentes. job session=%s",
                    email, job.get("session_id")
                )

        # Fallback: agent_drellia_id de envio_mensajes (podría ser el "principal")
        fallback = job.get("agent_drellia_id")
        if fallback:
            logger.warning(
                "[EMPLOYEE_RESOLVE] Usando fallback agent_drellia_id=%r para AGENT sin email mapeado.",
                fallback
            )
            return str(fallback)

        logger.error(
            "[EMPLOYEE_RESOLVE] No se pudo resolver employeeId para AGENT (email=%r, sin fallback). job session=%s",
            email, job.get("session_id")
        )
        return None

    logger.error(
        "[EMPLOYEE_RESOLVE] employee_type inesperado en segment: %s (job session=%s)",
        employee_type, job.get("session_id")
    )
    return None


# ========= BUILD MESSAGES PAYLOAD =========

def build_messages_body_for_segment(
    segment: ConversationSegment,
    customer_id: str,
    employee_id: str,
) -> List[Dict[str, Any]]:

    body: List[Dict[str, Any]] = []

    for m in segment.messages:
        if not m.content:
            continue

        # senderRole / senderId
        if m.actor_type == "CUSTOMER":
            sender_role = "customer"
            sender_id = customer_id
        elif m.actor_type in ("BOT", "AGENT"):
            sender_role = "employee"
            sender_id = employee_id
        else:
            # Por diseño, no deberíamos tener UNKNOWN aquí si el normalizer filtró bien
            logger.warning(
                "[BUILD_MSGS] Mensaje con actor_type=%s ignorado. raw=%r",
                m.actor_type, m.raw
            )
            continue

        # Timestamp: ya viene en ms
        ts_ms = int(m.ts_ms)

        body.append({
            "content": m.content,
            "senderRole": sender_role,
            "senderId": str(sender_id),
            "timestamp": ts_ms,
        })

    return body


# ========= ENVÍO A DRELLIA POR SEGMENTO =========

def send_segment_to_drellia(
    conn,
    job: Dict[str, Any],
    segment: ConversationSegment,
    customer_id: str,
) -> Dict[str, Any]:
    """
    Envía UN segmento (una conversación Drellia) y sus mensajes.

    Retorna un dict tipo:

    {
      "session_id": "...",
      "segment_index": 0,
      "employee_type": "BOT" | "AGENT",
      "employee_email": "...",
      "status": "SENT" | "FAILED",
      "reason": str | None,
      "http_code_conv": int,
      "http_code_msgs": int,
      "conv_id": str | None,
    }
    """

    session_id = job["session_id"]
    lote_id    = job.get("lote_id")
    provider_id = config.DRELLIA_PROVIDER_ID

    if not provider_id:
        msg = "Falta DRELLIA_PROVIDER_ID en config"
        logger.error("[SEND_SEGMENT][%s] %s", session_id, msg)
        return {
            "session_id": session_id,
            "segment_index": segment.segment_index,
            "employee_type": segment.employee_type,
            "employee_email": segment.employee_email,
            "status": "FAILED",
            "reason": msg,
            "http_code_conv": 0,
            "http_code_msgs": 0,
            "conv_id": None,
        }

    # 1) Resolver employeeId para el segmento
    employee_id = resolve_employee_id_for_segment(conn, job, segment)
    if not employee_id:
        reason = "EMPLOYEE_RESOLUTION_FAILED"
        logger.warning("[SEND_SEGMENT][%s] %s seg=%d", session_id, reason, segment.segment_index)
        return {
            "session_id": session_id,
            "segment_index": segment.segment_index,
            "employee_type": segment.employee_type,
            "employee_email": segment.employee_email,
            "status": "FAILED",
            "reason": reason,
            "http_code_conv": 0,
            "http_code_msgs": 0,
            "conv_id": None,
        }

    # 2) Build mensajes
    msgs_body = build_messages_body_for_segment(segment, customer_id, employee_id)

    if not msgs_body:
        reason = "NO_VALID_MESSAGES"
        logger.info(
            "[SEND_SEGMENT][%s] SKIPPED: no hay mensajes de texto válidos en segmento %d",
            session_id, segment.segment_index
        )
        return {
            "session_id": session_id,
            "segment_index": segment.segment_index,
            "employee_type": segment.employee_type,
            "employee_email": segment.employee_email,
            "status": "SKIPPED_NO_MESSAGES",
            "reason": reason,
            "http_code_conv": 0,
            "http_code_msgs": 0,
            "conv_id": None,
        }

    # 3) Crear conversación
    api_key = get_drellia_api_key()
    headers = {
        "x-drellia-audit-api-key": api_key,
        "Content-Type": "application/json",
    }

    conv_payload = {
        "providerId":       provider_id,
        "employeeId":       str(employee_id),
        "customerId":       str(customer_id),
        "originalDateTime": int(segment.original_ts_ms),
    }

    conv_url = f"{config.DRELLIA_BASE_URL.rstrip('/')}/v1/conversations"

    try:
        resp_conv = _post_with_retry(
            conv_url,
            headers,
            conv_payload,
            timeout=config.CONV_CREATE_TIMEOUT,
            max_retries=2,
            retry_on_5xx=True,
        )
    except requests.Timeout as e:
        reason = f"EXCEPTION_TIMEOUT_CONV: {e}"
        logger.error("[SEND_SEGMENT][%s] CONV_CREATE_TIMEOUT seg=%d: %s",
                     session_id, segment.segment_index, e)
        return {
            "session_id": session_id,
            "segment_index": segment.segment_index,
            "employee_type": segment.employee_type,
            "employee_email": segment.employee_email,
            "status": "FAILED",
            "reason": reason,
            "http_code_conv": 0,
            "http_code_msgs": 0,
            "conv_id": None,
        }

    if resp_conv.status_code not in (200, 201):
        logger.error(
            "[SEND_SEGMENT][%s] CONV_CREATE_FAILED seg=%d status=%s body=%s",
            session_id, segment.segment_index, resp_conv.status_code, resp_conv.text[:400]
        )
        return {
            "session_id": session_id,
            "segment_index": segment.segment_index,
            "employee_type": segment.employee_type,
            "employee_email": segment.employee_email,
            "status": "FAILED",
            "reason": "CONV_CREATE_FAILED",
            "http_code_conv": resp_conv.status_code,
            "http_code_msgs": 0,
            "conv_id": None,
        }

    try:
        body_conv = resp_conv.json()
    except Exception:
        body_conv = {}

    conv_data = body_conv.get("data") or body_conv
    conv_id = conv_data.get("id")
    if not conv_id:
        logger.error(
            "[SEND_SEGMENT][%s] CONV_CREATE_NO_ID seg=%d body=%s",
            session_id, segment.segment_index, resp_conv.text[:400]
        )
        return {
            "session_id": session_id,
            "segment_index": segment.segment_index,
            "employee_type": segment.employee_type,
            "employee_email": segment.employee_email,
            "status": "FAILED",
            "reason": "CONV_CREATE_NO_ID",
            "http_code_conv": resp_conv.status_code,
            "http_code_msgs": 0,
            "conv_id": None,
        }

    # 4) Enviar mensajes al endpoint de mensajes
    msgs_url = f"{config.DRELLIA_BASE_URL.rstrip('/')}/v1/conversations/{conv_id}/messages"

    try:
        resp_msgs = _post_with_retry(
            msgs_url,
            headers,
            msgs_body,
            timeout=config.MESSAGES_TIMEOUT,
            max_retries=2,
            retry_on_5xx=True,
        )
    except requests.Timeout as e:
        reason = f"EXCEPTION_TIMEOUT_MSGS: {e}"
        logger.error(
            "[SEND_SEGMENT][%s] MESSAGES_TIMEOUT seg=%d conv_id=%s: %s",
            session_id, segment.segment_index, conv_id, e
        )
        return {
            "session_id": session_id,
            "segment_index": segment.segment_index,
            "employee_type": segment.employee_type,
            "employee_email": segment.employee_email,
            "status": "FAILED",
            "reason": reason,
            "http_code_conv": resp_conv.status_code,
            "http_code_msgs": 0,
            "conv_id": conv_id,
        }

    if resp_msgs.status_code not in (200, 201):
        logger.error(
            "[SEND_SEGMENT][%s] MESSAGES_FAILED seg=%d status=%s body=%s",
            session_id, segment.segment_index, resp_msgs.status_code, resp_msgs.text[:400]
        )
        return {
            "session_id": session_id,
            "segment_index": segment.segment_index,
            "employee_type": segment.employee_type,
            "employee_email": segment.employee_email,
            "status": "FAILED",
            "reason": "MESSAGES_FAILED",
            "http_code_conv": resp_conv.status_code,
            "http_code_msgs": resp_msgs.status_code,
            "conv_id": conv_id,
        }

    logger.info(
        "[SEND_SEGMENT][%s] SENT seg=%d conv_id=%s mensajes=%d",
        session_id, segment.segment_index, conv_id, len(msgs_body)
    )

    # Log de ejemplo de payload para debug con el equipo de Drellia
    try:
        sample_msg = json.dumps(msgs_body[0], ensure_ascii=False)[:500] if msgs_body else "[]"
        logger.info(
            "[SEND_SEGMENT][%s] SAMPLE_MSG seg=%d conv_id=%s %s",
            session_id, segment.segment_index, conv_id, sample_msg
        )
    except Exception:
        pass

    return {
        "session_id": session_id,
        "segment_index": segment.segment_index,
        "employee_type": segment.employee_type,
        "employee_email": segment.employee_email,
        "status": "SENT",
        "reason": None,
        "http_code_conv": resp_conv.status_code,
        "http_code_msgs": resp_msgs.status_code,
        "conv_id": conv_id,
    }
