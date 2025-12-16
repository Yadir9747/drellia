# messages_normalizer.py
from __future__ import annotations

import json
import logging
import re
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional

import pandas as pd

from models import NormalizedMessage

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)


# ---------- TIMESTAMP PARSING ---------------

def parse_timestamp_to_ms(ts: Any) -> Optional[int]:
    if ts is None:
        return None

    # 1) Intento directo con pandas
    try:
        p = pd.to_datetime(ts, utc=True, errors="coerce")
        if not pd.isna(p):
            return int(p.value // 10**6)
    except Exception:
        pass

    # 2) Intento parseo tipo "YYYY, MM, DD, hh, mm, ss, micros"
    if isinstance(ts, str):
        m = re.match(
            r"\s*(\d{4}),\s*(\d{1,2}),\s*(\d{1,2}),\s*(\d{1,2}),"
            r"\s*(\d{1,2}),\s*(\d{1,2}),\s*(\d+),?\s*$",
            ts
        )
        if m:
            try:
                y, mo, d, h, mi, s, micro = map(int, m.groups())
                dt = datetime(y, mo, d, h, mi, s, micro, tzinfo=timezone.utc)
                return int(dt.timestamp() * 1000)
            except Exception:
                pass

    # 3) Intento si es datetime nativo
    if isinstance(ts, datetime):
        try:
            return int(ts.replace(tzinfo=timezone.utc).timestamp() * 1000)
        except Exception:
            pass

    logger.warning("parse_timestamp_to_ms: no se pudo parsear ts=%r", ts)
    return None


# ---------- ACTOR RESOLUTION ----------------

def resolve_actor(us_origen: Any, raw_msg: dict) -> (str, Optional[str]):
    """
    Devuelve (actor_type, actor_email).
    actor_type = "CUSTOMER" | "BOT" | "AGENT" | "UNKNOWN"
    actor_email = email del agente si aplica.
    """
    if not us_origen:
        return "UNKNOWN", None

    origin = str(us_origen).strip().lower()

    # Customer (Drellia: senderRole = customer)
    if origin in ("user", "cliente", "customer"):
        return "CUSTOMER", None

    # Bot
    if origin in ("bot", "flow", "ivr", "system"):
        return "BOT", None

    # Agente humano
    if origin in ("operator", "agente", "agent", "supervisor"):
        # intentamos sacar email si viene en raw
        email = raw_msg.get("operador_email") or raw_msg.get("agent_email")
        return "AGENT", email

    return "UNKNOWN", None


# ---------- PARSEO ESPECIAL: DUMP PYTHON --------------

def _try_parse_python_dump_string(s: str) -> Optional[List[Dict[str, Any]]]:
    """
    Intenta parsear un string que parece un dump de Python, del estilo:

    "[{'message_time': datetime.datetime(...), 'us_origen': 'user', 'mensaje': 'Hola', 'audios': None,
       'operador_nombre': '...', 'operador_email': '...', 'operador_rol': '...', 'departamento': '...'}
      {...}]"

    Ahora preservamos también operador_email, operador_nombre, operador_rol y departamento,
    para que luego puedan usarse en la resolución de agentes.
    """

    # Heurística: si no tiene estas claves, ni lo intentamos
    if "datetime.datetime" not in s or "'us_origen':" not in s or "'mensaje':" not in s:
        return None

    try:
        # Extraer cada "bloque" tipo { 'message_time': ... }
        dict_pattern = re.compile(r"\{([^{}]*)\}", re.DOTALL)
        matches = list(dict_pattern.finditer(s))
        if not matches:
            return None

        parsed_msgs: List[Dict[str, Any]] = []

        for m in matches:
            block = m.group(0)  # incluye las llaves

            # us_origen
            m_origin = re.search(r"'us_origen':\s*'([^']*)'", block)
            us_origen = m_origin.group(1) if m_origin else None

            # mensaje  (permitimos \' dentro del texto)
            m_msg = re.search(r"'mensaje':\s*'((?:\\'|[^'])*)'", block)
            if m_msg:
                msg_raw = m_msg.group(1)
                content = msg_raw.replace("\\'", "'")
            else:
                content = None

            # message_time: datetime.datetime(...)
            m_time = re.search(r"datetime\.datetime\(([^)]*)\)", block)
            dt_obj: Optional[datetime] = None
            if m_time:
                nums = m_time.group(1)
                # Ej: "2025, 12, 4, 0, 50, 54, 884000, tzinfo=<UTC>"
                parts = [p.strip() for p in nums.split(",")]
                ints: List[int] = []
                for p in parts:
                    if p.isdigit():
                        ints.append(int(p))
                    else:
                        # dejamos de leer cuando ya no son números
                        break
                if len(ints) >= 3:
                    year = ints[0]
                    month = ints[1]
                    day = ints[2]
                    hour = ints[3] if len(ints) > 3 else 0
                    minute = ints[4] if len(ints) > 4 else 0
                    second = ints[5] if len(ints) > 5 else 0
                    micro = ints[6] if len(ints) > 6 else 0
                    try:
                        dt_obj = datetime(
                            year, month, day,
                            hour, minute, second, micro,
                            tzinfo=timezone.utc
                        )
                    except Exception:
                        dt_obj = None

            # operador_email
            m_email = re.search(r"'operador_email':\s*'([^']*)'", block)
            operador_email = m_email.group(1) if m_email else None

            # operador_nombre
            m_nombre = re.search(r"'operador_nombre':\s*'([^']*)'", block)
            operador_nombre = m_nombre.group(1) if m_nombre else None

            # operador_rol
            m_rol = re.search(r"'operador_rol':\s*'([^']*)'", block)
            operador_rol = m_rol.group(1) if m_rol else None

            # departamento
            m_depto = re.search(r"'departamento':\s*'([^']*)'", block)
            departamento = m_depto.group(1) if m_depto else None

            parsed_msgs.append(
                {
                    "message_time": dt_obj,
                    "us_origen": us_origen,
                    "mensaje": content,
                    # Campos adicionales que necesitamos preservar:
                    "operador_email": operador_email,
                    "operador_nombre": operador_nombre,
                    "operador_rol": operador_rol,
                    "departamento": departamento,
                }
            )

        if not parsed_msgs:
            return None

        logger.info("[NORMALIZER] Se parseó dump Python en %d mensajes individuales", len(parsed_msgs))
        return parsed_msgs

    except Exception as e:
        logger.warning("[NORMALIZER] Error parseando dump Python de mensajes: %s", e)
        return None


# ---------- PARSEO DE MENSAJES --------------

def load_raw_messages(mensajes_raw: Any) -> List[Any]:

    # Caso lista nativa
    if isinstance(mensajes_raw, list):
        return mensajes_raw

    # Caso dict
    if isinstance(mensajes_raw, dict):
        # Caso especial: dict que contiene toda la conversación en msg["mensaje"] como dump Python
        if (
            "mensaje" in mensajes_raw
            and isinstance(mensajes_raw["mensaje"], str)
        ):
            inner = mensajes_raw["mensaje"].strip()
            parsed = _try_parse_python_dump_string(inner)
            if parsed is not None:
                return parsed

        # Caso normal: lo tratamos como un solo mensaje
        return [mensajes_raw]

    # Caso string
    if isinstance(mensajes_raw, str):
        s = mensajes_raw.strip()
        if not s:
            return []

        # 1) Intentar parsear como JSON correctamente
        try:
            parsed = json.loads(s)

            if isinstance(parsed, list):
                return parsed
            if isinstance(parsed, dict):
                return [parsed]
            if isinstance(parsed, str):
                # Intentamos parsear este string interno como dump Python
                inner_parsed = _try_parse_python_dump_string(parsed.strip())
                if inner_parsed is not None:
                    return inner_parsed
        except Exception:
            pass

        # 2) Intentar parsear el string original como dump Python
        python_parsed = _try_parse_python_dump_string(s)
        if python_parsed is not None:
            return python_parsed

        # 3) Último recurso: envolver como un único mensaje textual
        return [{
            "mensaje": s,
            "us_origen": "unknown",
            "message_time": None,
        }]

    logger.warning("[NORMALIZER] mensajes_raw tipo no reconocido: %s", type(mensajes_raw))
    return []


# ---------- NORMALIZACIÓN -------------------

def normalize_messages(mensajes_raw: Any) -> List[NormalizedMessage]:
    """
    Recibe la columna "mensajes" cruda del job y devuelve
    una lista ordenada de NormalizedMessage.
    """

    raw_items = load_raw_messages(mensajes_raw)
    output: List[NormalizedMessage] = []

    for msg in raw_items:
        if not isinstance(msg, dict):
            logger.warning("[NORMALIZER] msg no es dict: %r", msg)
            continue

        # CONTENT
        content = msg.get("mensaje")
        if content is None:
            continue

        content_str = str(content).strip()

        # Filtrar mensajes irrelevantes
        if not content_str:
            continue
        if content_str in ("__image__", "__audio__", "__file__", "<image>", "<audio>"):
            continue

        # TIMESTAMP
        ts = msg.get("message_time")
        ts_ms = parse_timestamp_to_ms(ts)
        if ts_ms is None:
            # fallback: usar tiempo actual si todo falló
            ts_ms = parse_timestamp_to_ms(datetime.utcnow())

        # ORIGEN / ACTOR
        us_origen = msg.get("us_origen")
        actor_type, actor_email = resolve_actor(us_origen, msg)

        if actor_type == "UNKNOWN":
            logger.warning("[NORMALIZER] Ignorando mensaje con actor UNKNOWN: %r", msg)
            continue

        # Crear objeto normalizado
        nm = NormalizedMessage(
            ts_ms=ts_ms,
            actor_type=actor_type,
            actor_email=actor_email,
            content=content_str,
            raw=msg,
        )
        output.append(nm)

    # Orden cronológico
    output.sort(key=lambda x: x.ts_ms)

    return output
