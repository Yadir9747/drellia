
# drellia_client.py
from __future__ import annotations

import json
import logging
from typing import Any, Dict, Optional, List

import requests

import config
import db
from models import ConversationSegment, NormalizedMessage  # ConversationSegment queda legacy

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


# RESOLVER EMPLOYEE POR ACTOR (BOT / AGENT)

def resolve_employee_id_for_actor(
    conn,
    job: Dict[str, Any],
    actor_type: str,
    actor_email: Optional[str],
) -> Optional[str]:

    session_id = job.get("session_id")

    if actor_type == "BOT":
        if config.DRELLIA_BOT_EMPLOYEE_ID:
            return str(config.DRELLIA_BOT_EMPLOYEE_ID)

        fallback = job.get("agent_drellia_id")
        if fallback:
            logger.warning(
                "[EMPLOYEE_RESOLVE] BOT sin DRELLIA_BOT_EMPLOYEE_ID, usando fallback agent_drellia_id=%r (session=%s)",
                fallback, session_id
            )
            return str(fallback)

        logger.error(
            "[EMPLOYEE_RESOLVE] No se pudo resolver employeeId para BOT (sin BOT_EMPLOYEE_ID ni fallback). session=%s",
            session_id
        )
        return None

    if actor_type == "AGENT":
        email_norm = (actor_email or "").strip().lower() or None
        if email_norm:
            drellia_uuid, _dept_id = db.get_agent_by_email(conn, email_norm)
            if drellia_uuid:
                return str(drellia_uuid)
            else:
                logger.warning(
                    "[EMPLOYEE_RESOLVE] No se encontró agente con email=%s en tabla agentes. session=%s",
                    email_norm, session_id
                )

        fallback = job.get("agent_drellia_id")
        if fallback:
            logger.warning(
                "[EMPLOYEE_RESOLVE] Usando fallback agent_drellia_id=%r para AGENT sin email mapeado. session=%s",
                fallback, session_id
            )
            return str(fallback)

        logger.error(
            "[EMPLOYEE_RESOLVE] No se pudo resolver employeeId para AGENT (email=%r, sin fallback). session=%s",
            actor_email, session_id
        )
        return None

    logger.error(
        "[EMPLOYEE_RESOLVE] actor_type inesperado en resolve_employee_id_for_actor: %s (session=%s)",
        actor_type, session_id
    )
    return None


# LEGACY: RESOLVER POR SEGMENTO (YA NO USADO POR main)

def resolve_employee_id_for_segment(
    conn,
    job: Dict[str, Any],
    segment: ConversationSegment,
) -> Optional[str]:
    employee_type = segment.employee_type
    email = (segment.employee_email or "").strip().lower() if segment.employee_email else None
    return resolve_employee_id_for_actor(conn, job, employee_type, email)


# ========= BUILD MESSAGES PAYLOAD POR SEGMENTO (LEGACY) =========

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
            logger.warning(
                "[BUILD_MSGS] Mensaje con actor_type=%s ignorado. raw=%r",
                m.actor_type, m.raw
            )
            continue

        ts_ms = int(m.ts_ms)

        body.append({
            "content": m.content,
            "senderRole": sender_role,
            "senderId": str(sender_id),
            "timestamp": ts_ms,
            "originalDateTime": ts_ms,
        })

    return body


#   ENVÍO DE TODA LA SESIÓN EN UNA SOLA CONVERSACIÓN

def _build_participants_from_messages(
    conn,
    job: Dict[str, Any],
    normalized_messages: List[NormalizedMessage],
) -> Dict[str, Dict[str, Any]]:

    participants: Dict[str, Dict[str, Any]] = {}

    # 1) Contar mensajes por actor
    for m in normalized_messages:
        if m.actor_type not in ("BOT", "AGENT"):
            continue

        if m.actor_type == "BOT":
            key = "BOT"
            email = None
        else:
            email = (m.actor_email or "").strip().lower() or None
            key = f"AGENT|{email}" if email else "AGENT|"

        if key not in participants:
            participants[key] = {
                "actor_type": m.actor_type,
                "email": email,
                "count": 0,
                "employee_id": None,
            }
        participants[key]["count"] += 1

    # 2) Resolver employee_id para cada participante
    for key, info in participants.items():
        actor_type = info["actor_type"]
        email = info["email"]
        employee_id = resolve_employee_id_for_actor(conn, job, actor_type, email)
        if not employee_id:
            logger.warning(
                "[PARTICIPANTS] No se pudo resolver employee_id para key=%s actor_type=%s email=%r",
                key, actor_type, email
            )
            info["employee_id"] = None
        else:
            info["employee_id"] = str(employee_id)

    return participants


def _build_messages_body_for_session(
    normalized_messages: List[NormalizedMessage],
    customer_id: str,
    participants: Dict[str, Dict[str, Any]],
    main_employee_id: str,
) -> List[Dict[str, Any]]:

    body: List[Dict[str, Any]] = []

    # Preprocesar: map de key -> employee_id
    key_to_emp_id: Dict[str, Optional[str]] = {}
    for key, info in participants.items():
        key_to_emp_id[key] = info.get("employee_id")

    for m in normalized_messages:
        if not m.content:
            continue

        # CUSTOMER
        if m.actor_type == "CUSTOMER":
            sender_role = "customer"
            sender_id = customer_id

        # BOT / AGENT
        elif m.actor_type in ("BOT", "AGENT"):
            if m.actor_type == "BOT":
                key = "BOT"
            else:
                email = (m.actor_email or "").strip().lower() or None
                key = f"AGENT|{email}" if email else "AGENT|"

            sender_role = "employee"
            sender_id = key_to_emp_id.get(key)

            if not sender_id:
                # Si no se pudo resolver el employee_id de este actor, saltamos el mensaje
                logger.warning(
                    "[BUILD_MSGS_SESSION] No employee_id para actor_type=%s key=%s; mensaje ignorado. raw=%r",
                    m.actor_type, key, m.raw
                )
                continue
        else:
            # UNKNOWN u otros, ignoramos
            logger.warning(
                "[BUILD_MSGS_SESSION] actor_type inesperado=%s; mensaje ignorado. raw=%r",
                m.actor_type, m.raw
            )
            continue

        ts_ms = int(m.ts_ms)

        body.append({
            "content": m.content,
            "senderRole": sender_role,
            "senderId": str(sender_id),
            "timestamp": ts_ms,
            "originalDateTime": ts_ms,
        })

    return body


def send_session_to_drellia(
    conn,
    job: Dict[str, Any],
    normalized_messages: List[NormalizedMessage],
    customer_id: str,
) -> Dict[str, Any]:


    session_id = job["session_id"]
    lote_id    = job.get("lote_id")
    provider_id = config.DRELLIA_PROVIDER_ID

    if not provider_id:
        msg = "Falta DRELLIA_PROVIDER_ID en config"
        logger.error("[SEND_SESSION][%s] %s", session_id, msg)
        return {
            "session_id": session_id,
            "lote_id": lote_id,
            "status": "FAILED",
            "reason": msg,
            "http_code_conv": 0,
            "http_code_msgs": 0,
            "segments": [],
        }

    # 1) Construir participantes empleados
    participants = _build_participants_from_messages(conn, job, normalized_messages)

    # Si no hay BOT ni AGENT, no podemos crear una conversación con employeeId
    if not participants:
        reason = "NO_EMPLOYEES_IN_SESSION"
        logger.warning("[SEND_SESSION][%s] SKIPPED: %s", session_id, reason)
        return {
            "session_id": session_id,
            "lote_id": lote_id,
            "status": "SKIPPED",
            "reason": reason,
            "http_code_conv": 0,
            "http_code_msgs": 0,
            "segments": [],
        }

    # 2) Elegir el employee principal: el que más mensajes tuvo
    valid_participants = {
        k: v for k, v in participants.items() if v.get("employee_id")
    }

    if not valid_participants:
        reason = "NO_RESOLVED_EMPLOYEE_IDS"
        logger.warning("[SEND_SESSION][%s] SKIPPED: %s", session_id, reason)
        return {
            "session_id": session_id,
            "lote_id": lote_id,
            "status": "SKIPPED",
            "reason": reason,
            "http_code_conv": 0,
            "http_code_msgs": 0,
            "segments": [],
        }

    main_key = max(
        valid_participants.keys(),
        key=lambda k: valid_participants[k]["count"]
    )
    main_employee_id = valid_participants[main_key]["employee_id"]

    logger.info(
        "[SEND_SESSION][%s] main_employee key=%s employee_id=%s count=%d",
        session_id,
        main_key,
        main_employee_id,
        valid_participants[main_key]["count"],
    )

    # 3) Crear conversación
    api_key = get_drellia_api_key()
    headers = {
        "x-drellia-audit-api-key": api_key,
        "Content-Type": "application/json",
    }

    # originalDateTime = timestamp del primer mensaje válido
    first_ts = min(m.ts_ms for m in normalized_messages) if normalized_messages else None
    original_dt = int(first_ts) if first_ts is not None else None

    conv_payload = {
        "providerId":       provider_id,
        "employeeId":       str(main_employee_id),
        "customerId":       str(customer_id),
    }
    if original_dt is not None:
        conv_payload["originalDateTime"] = original_dt

    try:
        logger.info(
            "[SEND_SESSION][%s] CONV_PAYLOAD %s",
            session_id,
            json.dumps(conv_payload, ensure_ascii=False),
        )
    except Exception:
        pass

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
        logger.error("[SEND_SESSION][%s] CONV_CREATE_TIMEOUT: %s", session_id, e)
        return {
            "session_id": session_id,
            "lote_id": lote_id,
            "status": "FAILED",
            "reason": reason,
            "http_code_conv": 0,
            "http_code_msgs": 0,
            "segments": [],
        }

    if resp_conv.status_code not in (200, 201):
        logger.error(
            "[SEND_SESSION][%s] CONV_CREATE_FAILED status=%s body=%s",
            session_id, resp_conv.status_code, resp_conv.text[:400]
        )
        return {
            "session_id": session_id,
            "lote_id": lote_id,
            "status": "FAILED",
            "reason": "CONV_CREATE_FAILED",
            "http_code_conv": resp_conv.status_code,
            "http_code_msgs": 0,
            "segments": [],
        }

    try:
        body_conv = resp_conv.json()
    except Exception:
        body_conv = {}

    conv_data = body_conv.get("data") or body_conv
    conv_id = conv_data.get("id")
    if not conv_id:
        logger.error(
            "[SEND_SESSION][%s] CONV_CREATE_NO_ID body=%s",
            session_id, resp_conv.text[:400]
        )
        return {
            "session_id": session_id,
            "lote_id": lote_id,
            "status": "FAILED",
            "reason": "CONV_CREATE_NO_ID",
            "http_code_conv": resp_conv.status_code,
            "http_code_msgs": 0,
            "segments": [],
        }

    # 4) Construir mensajes para TODA la sesión
    msgs_body = _build_messages_body_for_session(
        normalized_messages=normalized_messages,
        customer_id=customer_id,
        participants=participants,
        main_employee_id=str(main_employee_id),
    )

    if not msgs_body:
        reason = "NO_VALID_MESSAGES"
        logger.info(
            "[SEND_SESSION][%s] SKIPPED: no hay mensajes válidos para conv_id=%s",
            session_id, conv_id
        )
        return {
            "session_id": session_id,
            "lote_id": lote_id,
            "status": "SKIPPED",
            "reason": reason,
            "http_code_conv": resp_conv.status_code,
            "http_code_msgs": 0,
            "segments": [],
        }

    # 5) Enviar mensajes al endpoint de mensajes
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
            "[SEND_SESSION][%s] MESSAGES_TIMEOUT conv_id=%s: %s",
            session_id, conv_id, e
        )
        return {
            "session_id": session_id,
            "lote_id": lote_id,
            "status": "FAILED",
            "reason": reason,
            "http_code_conv": resp_conv.status_code,
            "http_code_msgs": 0,
            "segments": [],
        }

    if resp_msgs.status_code not in (200, 201):
        logger.error(
            "[SEND_SESSION][%s] MESSAGES_FAILED status=%s body=%s",
            session_id, resp_msgs.status_code, resp_msgs.text[:400]
        )
        return {
            "session_id": session_id,
            "lote_id": lote_id,
            "status": "FAILED",
            "reason": "MESSAGES_FAILED",
            "http_code_conv": resp_conv.status_code,
            "http_code_msgs": resp_msgs.status_code,
            "segments": [],
        }

    logger.info(
        "[SEND_SESSION][%s] SENT conv_id=%s mensajes=%d",
        session_id, conv_id, len(msgs_body)
    )

    # Log de muestra de un mensaje
    try:
        sample_msg = json.dumps(msgs_body[0], ensure_ascii=False)[:500] if msgs_body else "[]"
        logger.info(
            "[SEND_SESSION][%s] SAMPLE_MSG conv_id=%s %s",
            session_id, conv_id, sample_msg
        )
    except Exception:
        pass

    # 6) Resultado de sesión (formato compatible con main / update_envio_status)
    return {
        "session_id": session_id,
        "lote_id": lote_id,
        "status": "SENT",
        "reason": None,
        "http_code_conv": resp_conv.status_code,
        "http_code_msgs": resp_msgs.status_code,
        "segments": [
            {
                "conv_id": conv_id,
                "main_employee_key": main_key,
                "main_employee_id": main_employee_id,
                "n_messages": len(msgs_body),
            }
        ],
    }


# LEGACY: ENVÍO POR SEGMENTO (YA NO USADO POR main)

def send_segment_to_drellia(
    conn,
    job: Dict[str, Any],
    segment: ConversationSegment,
    customer_id: str,
) -> Dict[str, Any]:
    """
    (Legacy) Envía UN segmento (una conversación Drellia) y sus mensajes.
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

    try:
        logger.info(
            "[SEND_SEGMENT][%s] CONV_PAYLOAD seg=%d %s",
            session_id,
            segment.segment_index,
            json.dumps(conv_payload, ensure_ascii=False),
        )
    except Exception:
        pass

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
            "employee_type": segment.segment_index,
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
            "employee_type": segment.segment_type if hasattr(segment, "segment_type") else segment.segment_index,
            "employee_email": segment.employee_email,
            "status": "FAILED",
            "reason": "CONV_CREATE_NO_ID",
            "http_code_conv": resp_conv.status_code,
            "http_code_msgs": 0,
            "conv_id": None,
        }

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
            "employee_type": segment.segment_index,
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
            "employee_type": segment.segment_type if hasattr(segment, "segment_type") else segment.segment_index,
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
