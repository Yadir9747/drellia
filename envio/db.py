# db.py
from __future__ import annotations

import json
import logging
from typing import Any, Dict, List, Optional, Sequence, Tuple

from google.cloud import secretmanager
from google.cloud.sql.connector import Connector

import config

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

# Re-exports / alias de config para que sea más legible
PROJECT_ID          = config.PROJECT_ID
PG_INSTANCE_CONN    = config.PG_INSTANCE_CONN_NAME
PG_DB_SECRET_NAME   = config.PG_DB_SECRET_NAME
PG_DB_NAME          = config.PG_DB_NAME
PG_DB_SCHEMA        = config.PG_DB_SCHEMA

LOTES_TABLE_FQN     = config.LOTES_TABLE_FQN
ENVIO_TABLE_FQN     = config.ENVIO_TABLE_FQN
AGENTES_TABLE       = config.AGENTES_TABLE
CUSTOMERS_TABLE     = config.CUSTOMERS_TABLE
DEPT_TABLE          = config.DEPT_TABLE

_connector: Optional[Connector] = None


# ========= HELPERS GENERALES =========

def get_secret_text(secret_name: str) -> str:

    client = secretmanager.SecretManagerServiceClient()
    name = f"projects/{PROJECT_ID}/secrets/{secret_name}/versions/latest"
    resp = client.access_secret_version(request={"name": name})
    return resp.payload.data.decode("utf-8")


def get_pg_conn():
    """
    Retorna una conexión a Cloud SQL Postgres usando Cloud SQL Connector.
    Usa el secreto PG_DB_SECRET_NAME para obtener user/password/dbname.

    Se intenta dejar la conexión en autocommit=True.
    """
    global _connector
    if _connector is None:
        _connector = Connector()

    cred_json = json.loads(get_secret_text(PG_DB_SECRET_NAME))
    user = cred_json["user"]
    password = cred_json["password"]
    dbname = cred_json.get("dbname", PG_DB_NAME)

    conn = _connector.connect(
        PG_INSTANCE_CONN,
        "pg8000",
        user=user,
        password=password,
        db=dbname,
    )

    # Algunos drivers pueden no soportar autocommit, por eso el try/except
    try:
        conn.autocommit = True
    except Exception:
        pass

    return conn


# ========= QUERIES SOBRE envio_mensajes =========

def fetch_pending_from_envio(
    conn,
    lote_id: Optional[str] = None,
    limit: Optional[int] = None,
) -> List[Dict[str, Any]]:

    base_sql = f"""
    SELECT
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
      customer_drellia_id,
      mensajes,
      first_msg_ts_ms,
      last_msg_ts_ms
    FROM {ENVIO_TABLE_FQN}
    WHERE estado_envio = 'PENDING'
    """

    params: List[Any] = []
    if lote_id:
        base_sql += " AND lote_id = %s"
        params.append(lote_id)

    base_sql += " ORDER BY first_msg_ts_ms"

    if limit and limit > 0:
        base_sql += " LIMIT %s"
        params.append(limit)

    cur = conn.cursor()
    try:
        cur.execute(base_sql, tuple(params))
        rows = cur.fetchall()
    finally:
        cur.close()

    cols = [
        "lote_id",
        "lote_num",
        "session_id",
        "cedula",
        "telefono",
        "email",
        "nombre_cliente",
        "nombre_completo",
        "agent_email",
        "agent_drellia_id",
        "customer_drellia_id",
        "mensajes",
        "first_msg_ts_ms",
        "last_msg_ts_ms",
    ]
    convs = [dict(zip(cols, r)) for r in rows]

    logger.info(
        "[DB] fetch_pending_from_envio: lote_id=%s -> %d filas PENDING",
        lote_id, len(convs)
    )
    return convs


def update_envio_status(
    conn,
    lote_id: str,
    session_id: str,
    estado: str,
    http_code_conv: int,
    http_code_msgs: int,
    error_message: Optional[str],
    sent_ts_ms: int,
) -> None:
    """
    Actualiza el estado de una fila en envio_mensajes.
    """
    sql = f"""
    UPDATE {ENVIO_TABLE_FQN}
    SET estado_envio   = %s,
        http_code_conv = %s,
        http_code_msgs = %s,
        error_message  = %s,
        sent_ts        = NOW(),
        sent_ts_ms     = %s,
        updated_at     = NOW()
    WHERE lote_id      = %s
      AND session_id   = %s
    """
    cur = conn.cursor()
    try:
        cur.execute(
            sql,
            (
                estado,
                http_code_conv,
                http_code_msgs,
                error_message,
                sent_ts_ms,
                lote_id,
                session_id,
            ),
        )
    finally:
        cur.close()


# ========= CUSTOMER HELPERS =========

def lookup_customer_by_identification(
    conn,
    identification_number: str,
) -> Optional[str]:
    """
    Busca en customers el último drellia_id para un identification_number dado.
    """
    sql = f"""
    SELECT drellia_id
    FROM {CUSTOMERS_TABLE}
    WHERE identification_number = %s
    ORDER BY updated_on DESC NULLS LAST
    LIMIT 1
    """
    cur = conn.cursor()
    try:
        cur.execute(sql, (identification_number,))
        row = cur.fetchone()
    finally:
        cur.close()

    return str(row[0]) if row else None


def update_customer_in_envio(
    conn,
    lote_id: str,
    session_id: str,
    customer_id: str,
) -> None:
    """
    Actualiza envio_mensajes.customer_drellia_id para una combinación lote_id + session_id.
    """
    sql = f"""
    UPDATE {ENVIO_TABLE_FQN}
    SET customer_drellia_id = %s,
        updated_at          = NOW()
    WHERE lote_id   = %s
      AND session_id = %s
    """
    cur = conn.cursor()
    try:
        cur.execute(sql, (customer_id, lote_id, session_id))
    finally:
        cur.close()


# ========= RESUMEN DE LOTE =========

def insert_lote_summary(
    conn,
    *,
    lote_id: Optional[str],
    lote_num: Optional[int],
    envio_ts,
    envio_ts_ms: int,
    total: int,
    sent_ok: int,
    sent_failed: int,
    details: Dict[str, Any],
) -> None:

    envios_lote_resumen_table = f"{PG_DB_SCHEMA}.envios_lote_resumen"

    sql = f"""
    INSERT INTO {envios_lote_resumen_table} (
      lote_id,
      lote_num,
      envio_ts,
      envio_ts_ms,
      total_conversaciones,
      enviados_ok,
      enviados_error,
      detalles_json
    )
    VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
    """

    detalles_json = json.dumps(details, default=str)

    params = (
        lote_id,
        lote_num,
        envio_ts,
        envio_ts_ms,
        total,
        sent_ok,
        sent_failed,
        detalles_json,
    )

    cur = conn.cursor()
    try:
        cur.execute(sql, params)
    finally:
        cur.close()


# ========= AGENTES / DEPARTAMENTOS =========

def get_agent_by_email(
    conn,
    email: str,
) -> Tuple[Optional[str], Optional[int]]:

    sql = f"""
    SELECT drellia_uuid, id_departamento
    FROM {AGENTES_TABLE}
    WHERE LOWER(email) = LOWER(%s)
    LIMIT 1
    """
    cur = conn.cursor()
    try:
        cur.execute(sql, (email,))
        row = cur.fetchone()
    finally:
        cur.close()

    if not row:
        return None, None
    return row[0], row[1]


def get_department_drellia_id(
    conn,
    dept_id: int,
) -> Optional[str]:
    """
    Devuelve el drellia_id de un departamento a partir de su id interno.
    """
    sql = f"""
    SELECT drellia_id
    FROM {DEPT_TABLE}
    WHERE id = %s
    """
    cur = conn.cursor()
    try:
        cur.execute(sql, (dept_id,))
        row = cur.fetchone()
    finally:
        cur.close()

    return row[0] if row else None
