# config.py

from __future__ import annotations
import os

# --------- PROYECTO / GCP ---------

PROJECT_ID = os.getenv("PROJECT_ID", "data-323821")

# --------- POSTGRES (Cloud SQL) --------

# Nombre de la instancia de Cloud SQL (obligatorio)
PG_INSTANCE_CONN_NAME = os.environ["PG_INSTANCE_DRELLIA"]

# Secreto con user/password/dbname para PG
PG_DB_SECRET_NAME = os.getenv("PG_DB_SECRET_NAME", "PG_DB_SECRET_DRELLIA")

PG_DB_NAME   = os.getenv("PG_DB_NAME", "drellia")
PG_DB_SCHEMA = os.getenv("PG_DB_SCHEMA", "drellia")
# Tablas principales

LOTES_TABLE_FQN  = f"{PG_DB_SCHEMA}.lotes_conversaciones"
ENVIO_TABLE_FQN  = f"{PG_DB_SCHEMA}.envio_mensajes"
AGENTES_TABLE    = f"{PG_DB_SCHEMA}.agentes"
CUSTOMERS_TABLE  = f"{PG_DB_SCHEMA}.customers"
DEPT_TABLE       = f"{PG_DB_SCHEMA}.departamentos"

# Email por defecto para el bot (cuando no hay operador humano)
DEFAULT_AGENT_EMAIL = os.getenv("DEFAULT_AGENT_EMAIL", "bot_zenziya@zenziya.com")

# --------- DRELLIA API ---------

DRELLIA_PROVIDER_ID = os.getenv("DRELLIA_PROVIDER_ID")  # obligatorio a nivel negocio
DRELLIA_BASE_URL    = os.getenv("DRELLIA_BASE_URL", "https://api.drellia.com")

# Nombre del secreto con el API token de Drellia
DRELLIA_SECRET_NAME = os.getenv("DRELLIA_SECRET_NAME", "drellia_api_token")

DRELLIA_API_KEY_ENV = os.getenv("DRELLIA_API_KEY")

# Opcional: ID del empleado Drellia que representa al bot

DRELLIA_BOT_EMPLOYEE_ID = os.getenv("DRELLIA_BOT_EMPLOYEE_ID")

# --------- CONCURRENCIA / CHUNKS ---------
MAX_WORKERS = int(os.getenv("MAX_WORKERS", "32"))
CHUNK_SIZE  = int(os.getenv("CHUNK_SIZE", "200"))

TIMEOUT_ERROR_THRESHOLD = float(os.getenv("TIMEOUT_ERROR_THRESHOLD", "0.2"))

# --------- HTTP SESSION / TIMEOUTS ---------

HTTP_POOL_CONNECTIONS = int(os.getenv("HTTP_POOL_CONNECTIONS", "100"))
HTTP_POOL_MAXSIZE     = int(os.getenv("HTTP_POOL_MAXSIZE", "100"))
HTTP_MAX_RETRIES      = int(os.getenv("HTTP_MAX_RETRIES", "0"))


# Timeouts por defecto (segundos) para las llamadas a Drellia

CONV_CREATE_TIMEOUT   = int(os.getenv("CONV_CREATE_TIMEOUT", "30"))
MESSAGES_TIMEOUT      = int(os.getenv("MESSAGES_TIMEOUT", "60"))