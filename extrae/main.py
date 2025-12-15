# main.py (drellia_extract_lote)
from __future__ import annotations

import json
import logging
import os
import uuid
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional

import pandas as pd
import functions_framework
from google.cloud import bigquery, secretmanager
from google.cloud.sql.connector import Connector

# --- CONFIG ---

PROJECT_ID = os.environ.get("PROJECT_ID", "data-323821")

# BigQuery (Botmaker)
BQ_LOCATION = os.environ.get("BQ_LOCATION")  # opcional
BOTMAKER_BQ_SECRET = os.environ.get("BOTMAKER_BQ_SECRET", "botmaker-key")

# SQL local (query de extracción)
SQL_FILE = os.environ.get("SQL_FILE", "drellia.sql")

# Ventana por defecto (horas)
WINDOW_HOURS_DEFAULT = int(os.environ.get("WINDOW_HOURS", "12"))

# Cloud SQL Postgres
PG_INSTANCE_CONN_NAME = os.environ["PG_INSTANCE_DRELLIA"]
PG_DB_SECRET_NAME = os.environ.get("PG_DB_SECRET_NAME", "PG_DB_SECRET_DRELLIA")
PG_DB_NAME = os.environ.get("PG_DB_NAME", "drellia")
PG_DB_SCHEMA = os.environ.get("PG_DB_SCHEMA", "drellia")

# Tabla de staging (lotes)
LOTES_TABLE_FQN = f"{PG_DB_SCHEMA}.lotes_conversaciones"

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

_connector: Optional[Connector] = None


# --- HELPERS GENERALES ---

def now_utc() -> datetime:
    return datetime.now(timezone.utc)


def now_ms() -> int:
    return int(now_utc().timestamp() * 1000)


def ts_to_epoch_ms(ts: Any) -> Optional[int]:
    if ts is None:
        return None
    try:
        p = pd.to_datetime(ts, utc=True, errors="coerce")
        if pd.isna(p):
            return None
        return int(p.value // 10**6)
    except Exception as e:
        logger.warning("ts_to_epoch_ms error: %s (value=%r)", e, ts)
        return None


def normalize_array(val: Any) -> List[Any]:
    if val is None:
        return []
    if isinstance(val, list):
        return val
    if isinstance(val, tuple):
        return list(val)
    if hasattr(val, "tolist"):
        try:
            return list(val.tolist())
        except Exception:
            pass
    return [val]


def load_sql(path: str, window_hours: int) -> str:
    with open(path, "r", encoding="utf-8") as f:
        txt = f.read()
    return txt.format(window_hours=window_hours)


# --- SECRET MANAGER / CLIENTES ---

def get_secret_text(secret_name: str) -> str:
    sm = secretmanager.SecretManagerServiceClient()
    full_name = f"projects/{PROJECT_ID}/secrets/{secret_name}/versions/latest"
    resp = sm.access_secret_version(request={"name": full_name})
    return resp.payload.data.decode("utf-8")


def get_botmaker_bq_client() -> bigquery.Client:
    payload = get_secret_text(BOTMAKER_BQ_SECRET)
    creds_info = json.loads(payload)
    from google.oauth2.service_account import Credentials

    creds = Credentials.from_service_account_info(
        creds_info,
        scopes=["https://www.googleapis.com/auth/cloud-platform"],
    )
    return bigquery.Client(project=PROJECT_ID, credentials=creds)


def get_pg_conn():
    global _connector
    if _connector is None:
        _connector = Connector()

    cred_json = json.loads(get_secret_text(PG_DB_SECRET_NAME))
    user = cred_json["user"]
    password = cred_json["password"]
    dbname = cred_json.get("dbname", PG_DB_NAME)

    conn = _connector.connect(
        PG_INSTANCE_CONN_NAME,
        "pg8000",
        user=user,
        password=password,
        db=dbname,
    )
    return conn


def get_next_lote_num(conn) -> int:
    sql = f"SELECT COALESCE(MAX(lote_num), 0) + 1 FROM {LOTES_TABLE_FQN}"
    cur = conn.cursor()
    try:
        cur.execute(sql)
        row = cur.fetchone()
    finally:
        cur.close()
    return int(row[0]) if row and row[0] is not None else 1


# --- CORE: EXTRAER DE BQ Y CARGAR EN CLOUD SQL ---

def extract_botmaker_sessions(window_hours: int) -> pd.DataFrame:
    bq_conv = get_botmaker_bq_client()
    sql = load_sql(SQL_FILE, window_hours)
    
    job = (
        bq_conv.query(sql, location=BQ_LOCATION)
        if BQ_LOCATION
        else bq_conv.query(sql)
    )
    result = job.result()
    df = result.to_dataframe(create_bqstorage_client=False)
    
    logger.info("Query BQ (Botmaker) OK, rows=%d", len(df))
    return df


def insert_lote_into_pg(df: pd.DataFrame, lote_id: uuid.UUID, lote_num: int) -> int:
    if df.empty:
        return 0

    lote_created_ms = now_ms()
    rows_to_insert: List[tuple] = []

    for _, row in df.iterrows():
        r = row.to_dict()
        session_id = r.get("session_id")

        if not session_id:
            continue

        cedula = r.get("cedula")
        cola_atencion = r.get("cola_atencion")
        nombre_cliente = r.get("nombre_cliente")
        nombre_completo = r.get("nombre_completo")
        email = r.get("email")
        telefono = r.get("telefono")
        nombre_agente_bm = r.get("nombre_agente_bm")

        mensajes_count = int(r.get("mensajes_count") or 0)
        mensajes_usuario = int(r.get("mensajes_usuario") or 0)
        mensajes_sistema = int(r.get("mensajes_sistema") or 0)

        tiene_audio = bool(r.get("tiene_audio"))
        audios_count = int(r.get("audios_count") or 0)

        session_creation_ms = ts_to_epoch_ms(r.get("session_creation_time"))
        first_msg_ts_ms = ts_to_epoch_ms(r.get("first_msg_ts"))
        last_msg_ts_ms = ts_to_epoch_ms(r.get("last_msg_ts"))

        departamentos_distintos = normalize_array(r.get("departamentos_distintos"))
        operadores_distintos = normalize_array(r.get("operadores_distintos"))
        operadores_emails_distintos = normalize_array(r.get("operadores_emails_distintos"))
        operadores_roles_distintos = normalize_array(r.get("operadores_roles_distintos"))

        mensajes_raw = r.get("mensajes")
        convs_por_agente_raw = r.get("conversaciones_por_agente")

        mensajes_json = (
            json.dumps(mensajes_raw, default=str) if mensajes_raw is not None else None
        )
        convs_json = (
            json.dumps(convs_por_agente_raw, default=str)
            if convs_por_agente_raw is not None
            else None
        )

        rows_to_insert.append((
            str(lote_id),
            lote_num,
            lote_created_ms,
            None,
            cedula,
            session_id,
            session_creation_ms,
            cola_atencion,
            nombre_cliente,
            nombre_completo,
            email,
            telefono,
            nombre_agente_bm,
            mensajes_count,
            mensajes_usuario,
            mensajes_sistema,
            first_msg_ts_ms,
            last_msg_ts_ms,
            departamentos_distintos or None,
            operadores_distintos or None,
            operadores_emails_distintos or None,
            operadores_roles_distintos or None,
            tiene_audio,
            audios_count,
            mensajes_json,
            convs_json,
        ))

    if not rows_to_insert:
        return 0

    insert_sql = f"""
    INSERT INTO {LOTES_TABLE_FQN} (
        lote_id,
        lote_num,
        lote_created_ms,
        lote_sent_ms,
        cedula,
        session_id,
        session_creation_ms,
        cola_atencion,
        nombre_cliente,
        nombre_completo,
        email,
        telefono,
        nombre_agente_bm,
        mensajes_count,
        mensajes_usuario,
        mensajes_sistema,
        first_msg_ts_ms,
        last_msg_ts_ms,
        departamentos_distintos,
        operadores_distintos,
        operadores_emails_distintos,
        operadores_roles_distintos,
        tiene_audio,
        audios_count,
        mensajes,
        conversaciones_por_agente
    )
    VALUES (
        %s,  -- lote_id
        %s,  -- lote_num
        %s,  -- lote_created_ms
        %s,  -- lote_sent_ms
        %s,  -- cedula
        %s,  -- session_id
        %s,  -- session_creation_ms
        %s,  -- cola_atencion
        %s,  -- nombre_cliente
        %s,  -- nombre_completo
        %s,  -- email
        %s,  -- telefono
        %s,  -- nombre_agente_bm
        %s,  -- mensajes_count
        %s,  -- mensajes_usuario
        %s,  -- mensajes_sistema
        %s,  -- first_msg_ts_ms
        %s,  -- last_msg_ts_ms
        %s,  -- departamentos_distintos
        %s,  -- operadores_distintos
        %s,  -- operadores_emails_distintos
        %s,  -- operadores_roles_distintos
        %s,  -- tiene_audio
        %s,  -- audios_count
        %s,  -- mensajes
        %s   -- conversaciones_por_agente
    );
    """

    conn = get_pg_conn()
    inserted = 0
    try:
        cur = conn.cursor()
        try:
            cur.executemany(insert_sql, rows_to_insert)
        finally:
            cur.close()
        conn.commit()
        inserted = len(rows_to_insert)
    finally:
        try:
            conn.close()
        except Exception:
            pass

    logger.info("Insertadas %d filas en %s", inserted, LOTES_TABLE_FQN)
    return inserted


# --- HTTP ENTRYPOINT ---

@functions_framework.http
def drellia_extract_lote(request):
    if request.method not in ("GET", "POST"):
        return ("ok", 200)

    try:
        args = request.args or {}
        wh_param = args.get("window_hours")
        window_hours = int(wh_param) if wh_param else WINDOW_HOURS_DEFAULT

        # 1) lote_id: si no viene, generamos uno nuevo
        lote_id_param = args.get("lote_id")
        if lote_id_param:
            lote_id = uuid.UUID(lote_id_param)
        else:
            lote_id = uuid.uuid4()

        # 2) lote_num: si no viene, calculamos MAX(lote_num)+1
        lote_num_param = args.get("lote_num")
        if lote_num_param:
            lote_num = int(lote_num_param)
        else:
            conn_for_num = get_pg_conn()
            try:
                lote_num = get_next_lote_num(conn_for_num)
            finally:
                try:
                    conn_for_num.close()
                except Exception:
                    pass

        logger.info(
            "Iniciando extracción Botmaker → Cloud SQL. window_hours=%d, lote_id=%s, lote_num=%d",
            window_hours,
            lote_id,
            lote_num,
        )

        # 3) Extraer de Botmaker
        df = extract_botmaker_sessions(window_hours=window_hours)

        if df.empty:
            result = {
                "status": "NO_DATA",
                "message": f"Sin sesiones en las últimas {window_hours} horas",
                "window_hours": window_hours,
                "lote_id": str(lote_id),
                "lote_num": lote_num,
                "rows_extracted": 0,
                "rows_inserted": 0,
            }
            return (json.dumps(result), 200, {"Content-Type": "application/json"})

        # 4) Insertar en lotes_conversaciones
        inserted = insert_lote_into_pg(df, lote_id=lote_id, lote_num=lote_num)

        result = {
            "status": "OK",
            "window_hours": window_hours,
            "lote_id": str(lote_id),
            "lote_num": lote_num,
            "rows_extracted": int(len(df)),
            "rows_inserted": int(inserted),
        }

        # 5) Hook opcional a enviar_analisis (email + gemini)
        try:
            import enviar_analisis
            if hasattr(enviar_analisis, "run_analisis"):
                try:
                    enviar_analisis.run_analisis(
                        lote_id=str(lote_id),
                        lote_num=lote_num,
                        window_hours=window_hours,
                        stats=result,
                    )
                    logger.info("enviar_analisis.run_analisis ejecutado para lote_id=%s", lote_id)
                except Exception as e_ana:
                    logger.warning("Error ejecutando enviar_analisis.run_analisis: %s", e_ana)
            else:
                logger.info("enviar_analisis importado pero sin función run_analisis()")
        except ImportError:
            logger.info("enviar_analisis.py no encontrado; se omite análisis posterior")

        # 6) Hook opcional a tabla_envio_mensajes (prepara tabla de envío)
        try:
            import tabla_envio_mensajes
            if hasattr(tabla_envio_mensajes, "run_tabla_envio_mensajes"):
                try:
                    tabla_envio_mensajes.run_tabla_envio_mensajes(
                        lote_id=str(lote_id),
                        lote_num=lote_num,
                    )
                    logger.info(
                        "tabla_envio_mensajes.run_tabla_envio_mensajes ejecutado para lote_id=%s",
                        lote_id,
                    )
                except Exception as e_tab:
                    logger.warning(
                        "Error ejecutando tabla_envio_mensajes.run_tabla_envio_mensajes: %s",
                        e_tab,
                    )
            else:
                logger.info(
                    "tabla_envio_mensajes importado pero sin función run_tabla_envio_mensajes()"
                )
        except ImportError:
            logger.info(
                "tabla_envio_mensajes.py no encontrado; se omite construcción de tabla de envío"
            )

        return (json.dumps(result), 200, {"Content-Type": "application/json"})

    except Exception as e:
        logger.exception("Error en drellia_extract_lote")
        result = {
            "status": "ERROR",
            "message": str(e),
        }
        return (json.dumps(result), 500, {"Content-Type": "application/json"})