# tabla_envio_mensajes.py
from __future__ import annotations

import json
import logging
import os
import re
import time
from typing import Any, Dict, List, Optional, Tuple

from google.cloud import secretmanager
from google.cloud.sql.connector import Connector

# --- CONFIG ---

PROJECT_ID = os.environ.get("PROJECT_ID", "data-323821")

PG_INSTANCE_CONN_NAME = os.environ["PG_INSTANCE_DRELLIA"]
PG_DB_SECRET_NAME = os.environ.get("PG_DB_SECRET_NAME", "PG_DB_SECRET_DRELLIA")
PG_DB_NAME = os.environ.get("PG_DB_NAME", "drellia")
PG_DB_SCHEMA = os.environ.get("PG_DB_SCHEMA", "drellia")

LOTES_TABLE_FQN = f"{PG_DB_SCHEMA}.lotes_conversaciones"
ENVIO_TABLE_FQN = f"{PG_DB_SCHEMA}.envio_mensajes"
AGENTES_TABLE = f"{PG_DB_SCHEMA}.agentes"
CUSTOMERS_TABLE = f"{PG_DB_SCHEMA}.customers"
DEPT_TABLE = f"{PG_DB_SCHEMA}.departamentos"

DEFAULT_AGENT_EMAIL = "bot_zenziya@zenziya.com"

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

_connector: Optional[Connector] = None


# --- HELPERS ---

def get_secret_text(name: str) -> str:
    sm = secretmanager.SecretManagerServiceClient()
    full = f"projects/{PROJECT_ID}/secrets/{name}/versions/latest"
    resp = sm.access_secret_version(request={"name": full})
    return resp.payload.data.decode("utf-8")


def get_pg_conn():
    global _connector
    if _connector is None:
        _connector = Connector()

    cred = json.loads(get_secret_text(PG_DB_SECRET_NAME))
    conn = _connector.connect(
        PG_INSTANCE_CONN_NAME,
        "pg8000",
        user=cred["user"],
        password=cred["password"],
        db=cred.get("dbname", PG_DB_NAME),
    )
    return conn


def normalize_phone(phone: Optional[str]) -> Optional[str]:
    if not phone:
        return None
    return re.sub(r"\D+", "", str(phone))


def now_ms() -> int:
    return int(time.time() * 1000)


# --- RESOLVERS ---

def resolve_agent(
    conn, operadores_emails: List[str]
) -> Tuple[str, Optional[str], Optional[int]]:
    """
    Retorna:
    - email_final
    - agent_uuid
    - id_departamento
    """
    if operadores_emails and operadores_emails[0]:
        email = operadores_emails[0].strip().lower()
    else:
        email = DEFAULT_AGENT_EMAIL

    sql = f"""
    SELECT drellia_uuid, id_departamento
    FROM {AGENTES_TABLE}
    WHERE LOWER(email) = LOWER(%s)
    LIMIT 1
    """
    cur = conn.cursor()
    cur.execute(sql, (email,))
    row = cur.fetchone()
    cur.close()

    if row:
        return email, row[0], row[1]
    else:
        return email, None, None


def resolve_departamento(conn, id_depto: Optional[int]) -> Optional[str]:
    if id_depto is None:
        return None

    sql = f"""
    SELECT drellia_id
    FROM {DEPT_TABLE}
    WHERE id = %s
    """
    cur = conn.cursor()
    cur.execute(sql, (id_depto,))
    row = cur.fetchone()
    cur.close()

    return row[0] if row else None


def resolve_customer(conn, telefono: Optional[str]) -> Optional[str]:
    clean = normalize_phone(telefono)
    if not clean:
        return None

    sql = f"""
    SELECT drellia_id
    FROM {CUSTOMERS_TABLE}
    WHERE identification_number = %s
    ORDER BY updated_on DESC NULLS LAST
    LIMIT 1
    """
    cur = conn.cursor()
    cur.execute(sql, (clean,))
    row = cur.fetchone()
    cur.close()

    return row[0] if row else None


# --- MAIN ---

def preparar_envio_mensajes_lote(lote_id_param: str) -> Dict[str, Any]:
    conn = get_pg_conn()
    try:
        # 1) Leer TODAS las conversaciones pendientes (de todos los lotes)
        sql_lotes = f"""
        SELECT
          lote_id,
          lote_num,
          session_id,
          cedula,
          telefono,
          email,
          nombre_cliente,
          nombre_completo,
          operadores_emails_distintos,
          mensajes,
          first_msg_ts_ms,
          last_msg_ts_ms
        FROM {LOTES_TABLE_FQN}
        WHERE lote_sent_ms IS NULL
        """
        cur = conn.cursor()
        cur.execute(sql_lotes)
        rows = cur.fetchall()
        cur.close()

        if not rows:
            logger.info(
                "[ENVIO] No hay conversaciones pendientes (lote_sent_ms IS NULL)"
            )
            return {"status": "NO_DATA", "lote_id": lote_id_param, "rows": 0}

        cols = [
            "lote_id",
            "lote_num",
            "session_id",
            "cedula",
            "telefono",
            "email",
            "nombre_cliente",
            "nombre_completo",
            "operadores_emails_distintos",
            "mensajes",
            "first_msg_ts_ms",
            "last_msg_ts_ms",
        ]
        convs = [dict(zip(cols, r)) for r in rows]

        logger.info("[ENVIO] Conversaciones pendientes totales: %d", len(convs))

        # 2) Limpiar tabla de envíos (inicio de corrida)
        cur = conn.cursor()
        cur.execute(f"DELETE FROM {ENVIO_TABLE_FQN};")
        cur.close()
        conn.commit()

        logger.info("[ENVIO] Tabla %s limpiada", ENVIO_TABLE_FQN)

        # 3) Insertar evitando duplicados por session_id (global)
        inserted = 0
        seen_sessions: set[str] = set()
        inserted_sessions: List[str] = []

        insert_sql = f"""
        INSERT INTO {ENVIO_TABLE_FQN} (
          lote_id,
          lote_num,
          session_id,
          cedula,
          telefono,
          email_cliente,
          nombre_cliente,
          nombre_completo,
          agent_email,
          agent_drellia_id,
          departamento_drellia_id,
          customer_drellia_id,
          mensajes,
          first_msg_ts_ms,
          last_msg_ts_ms,
          estado_envio,
          http_code_conv,
          http_code_msgs,
          error_message,
          faltan_datos,
          faltan_detalle
        )
        VALUES (
          %s, %s, %s,
          %s, %s, %s,
          %s, %s,
          %s, %s,
          %s, %s,
          %s, %s, %s,
          %s,
          %s, %s, %s,
          %s, %s
        )
        """

        cur = conn.cursor()

        for c in convs:
            sid = c["session_id"]
            if not sid:
                continue
            if sid in seen_sessions:
                continue
            
            seen_sessions.add(sid)
            lote_id_row = c["lote_id"]
            lote_num = c["lote_num"]

            # Resolver agente
            operadores_emails = c.get("operadores_emails_distintos") or []
            agent_email, agent_uuid, id_depto = resolve_agent(conn, operadores_emails)

            # Resolver depto
            dept_uuid = resolve_departamento(conn, id_depto)

            # Resolver customer
            customer_uuid = resolve_customer(conn, c["telefono"])

            # Flags de faltantes
            faltantes: List[str] = []
            if agent_uuid is None:
                faltantes.append("agent")
            if dept_uuid is None:
                faltantes.append("department")
            if customer_uuid is None:
                faltantes.append("customer")

            faltan_datos = bool(faltantes)
            faltan_detalle = ", ".join(faltantes) if faltantes else None

            # Serializar mensajes a JSON válido
            raw_mensajes = c["mensajes"]
            if isinstance(raw_mensajes, (dict, list)):
                mensajes_json = json.dumps(
                    raw_mensajes, ensure_ascii=False, default=str
                )
            elif isinstance(raw_mensajes, str):
                try:
                    json.loads(raw_mensajes)
                    mensajes_json = raw_mensajes
                except json.JSONDecodeError:
                    mensajes_json = json.dumps(raw_mensajes, ensure_ascii=False)
            else:
                mensajes_json = json.dumps(
                    raw_mensajes, ensure_ascii=False, default=str
                )

            cur.execute(
                insert_sql,
                (
                    lote_id_row,
                    lote_num,
                    sid,
                    c["cedula"],
                    c["telefono"],
                    c["email"],
                    c["nombre_cliente"],
                    c["nombre_completo"],
                    agent_email,
                    agent_uuid,
                    dept_uuid,
                    customer_uuid,
                    mensajes_json,
                    c["first_msg_ts_ms"],
                    c["last_msg_ts_ms"],
                    "PENDING",
                    None,  # http_code_conv
                    None,  # http_code_msgs
                    None,  # error_message
                    faltan_datos,
                    faltan_detalle,
                ),
            )
            inserted += 1
            inserted_sessions.append(sid)

        cur.close()
        conn.commit()

        logger.info(
            "[ENVIO] Insertadas %d filas en %s (session_id distintos=%d)",
            inserted,
            ENVIO_TABLE_FQN,
            len(seen_sessions),
        )

        # 4) Marcar lote_sent_ms para las sesiones procesadas
        if inserted_sessions:
            cur = conn.cursor()
            cur.execute(
                f"""
                UPDATE {LOTES_TABLE_FQN}
                SET lote_sent_ms = %s
                WHERE lote_sent_ms IS NULL
                  AND session_id = ANY(%s)
                """,
                (now_ms(), inserted_sessions),
            )
            updated = cur.rowcount
            cur.close()
            conn.commit()

            logger.info(
                "[ENVIO] Marcadas %d filas en %s con lote_sent_ms (pendientes procesadas)",
                updated,
                LOTES_TABLE_FQN,
            )

        return {
            "status": "OK",
            "lote_id_param": lote_id_param,
            "rows_pending": len(convs),
            "rows_inserted": inserted,
            "sessions_distinct": len(seen_sessions),
        }

    finally:
        try:
            conn.close()
        except Exception:
            pass


# --- Wrapper llamado desde main.py ---

def run_tabla_envio_mensajes(lote_id: str, lote_num: int):
    logger.info(
        "[ENVIO] run_tabla_envio_mensajes llamado con lote_id=%s, lote_num=%s (solo para logging)",
        lote_id,
        lote_num,
    )
    return preparar_envio_mensajes_lote(lote_id)