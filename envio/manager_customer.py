# manager_customer.py
from __future__ import annotations

import logging
import os
from datetime import datetime, timezone
from typing import Any, Dict, Optional, Tuple

import requests

logger = logging.getLogger(__name__)

if not logger.handlers:
    logging.basicConfig(level=logging.INFO)

logger.setLevel(logging.INFO)

# --- CONFIG ---

PROJECT_ID = os.environ.get("PROJECT_ID", "data-323821")
DRELLIA_BASE_URL = os.environ.get("DRELLIA_BASE_URL", "https://api.drellia.com")
DRELLIA_SECRET_NAME = os.environ.get("DRELLIA_SECRET_NAME", "drellia_api_token")

# Teléfono por defecto cuando no hay info
DEFAULT_PHONE = "00000000000"
DEFAULT_FIRST_NAME = "Customer"
DEFAULT_LAST_NAME = "Default"

_session = requests.Session()


# --- HELPERS GENERALES ---

def now_utc() -> datetime:
    return datetime.now(timezone.utc)


def normalize_phone(phone: Optional[str]) -> str:
    if not phone:
        return DEFAULT_PHONE
    digits = "".join(ch for ch in str(phone) if ch.isdigit())
    if not digits:
        return DEFAULT_PHONE
    return digits


def split_name(nombre: Optional[str]) -> Tuple[str, str]:
    if not nombre:
        return DEFAULT_FIRST_NAME, DEFAULT_LAST_NAME

    base = " ".join(str(nombre).strip().split())
    if not base:
        return DEFAULT_FIRST_NAME, DEFAULT_LAST_NAME

    parts = base.split()
    if len(parts) == 1:
        return parts[0], parts[0]
    return parts[0], " ".join(parts[1:])


def _get_api_key_from_secret() -> str:
    env_key = os.environ.get("DRELLIA_API_KEY")
    if env_key and env_key.strip():
        return env_key.strip()

    from google.cloud import secretmanager
    sm = secretmanager.SecretManagerServiceClient()
    name = f"projects/{PROJECT_ID}/secrets/{DRELLIA_SECRET_NAME}/versions/latest"
    payload = sm.access_secret_version(request={"name": name}).payload.data.decode("utf-8")
    api_key = payload.strip()
    if not api_key:
        raise RuntimeError("El secreto de Drellia está vacío")
    return api_key


def _iso_to_dt(s: Optional[str]) -> Optional[datetime]:
    """
    Convierte un string ISO con posible sufijo 'Z' en datetime con TZ UTC.
    """
    if not s:
        return None
    try:
        return datetime.fromisoformat(s.replace("Z", "+00:00"))
    except Exception:
        return None


# --- LLAMADAS A DRELLIA /v1/customers ---

def _find_customer_http(
    *,
    external_id: Optional[str],
    email: Optional[str],
    identification_number: Optional[str],
) -> Optional[Tuple[str, datetime, datetime]]:

    api_key = _get_api_key_from_secret()
    headers = {
        "x-drellia-audit-api-key": api_key,
        "Content-Type": "application/json",
    }

    base_url = f"{DRELLIA_BASE_URL.rstrip('/')}/v1/customers"

    # Lista de (campo_api, valor_buscado, clave_en_json, normalizador)
    search_fields = [
        ("externalId", external_id, "externalId", lambda x: str(x) if x is not None else None),
        ("email", email, "email", lambda x: str(x).lower() if x is not None else None),
        ("identificationNumber", identification_number, "identificationNumber", lambda x: str(x) if x is not None else None),
    ]

    for field, value, json_key, normalize in search_fields:
        if not value:
            continue

        params: Dict[str, Any] = {"page": 1, "limit": 50, field: value}

        try:
            resp = _session.get(base_url, headers=headers, params=params, timeout=20)
        except Exception as e:
            logger.error(
                "[DRELLIA][CUSTOMER] Excepción en GET /v1/customers (%s=%s): %s",
                field, value, e
            )
            continue

        if resp.status_code != 200:
            logger.warning(
                "[DRELLIA][CUSTOMER] GET /v1/customers (%s=%s) status=%s body=%s",
                field, value, resp.status_code, resp.text[:400]
            )
            continue

        try:
            body = resp.json()
        except Exception:
            logger.error(
                "[DRELLIA][CUSTOMER] Respuesta no JSON en GET (%s=%s): %s",
                field, value, resp.text[:400]
            )
            continue

        data_block = body.get("data")
        if isinstance(data_block, dict) and "results" in data_block:
            results = data_block.get("results") or []
        else:
            results = body.get("results") or []

        if not results:
            continue

        # Normalizamos el valor buscado para compararlo
        target = normalize(value)

        matched_customer = None
        for cust in results:
            candidate = normalize(cust.get(json_key))
            if candidate == target:
                matched_customer = cust
                break

        if not matched_customer:
            # No hay ningún customer cuyo campo json_key coincida exactamente
            logger.info(
                "[DRELLIA][CUSTOMER] GET /v1/customers (%s=%s) devolvió resultados, pero ninguno coincide exactamente en %s",
                field, value, json_key
            )
            continue

        cust_id = matched_customer.get("id")
        if not cust_id:
            continue

        created_on = _iso_to_dt(matched_customer.get("createdOn")) or now_utc()
        updated_on = _iso_to_dt(matched_customer.get("updatedOn")) or created_on

        logger.info(
            "[DRELLIA][CUSTOMER] Encontrado id=%s usando filtro %s=%s (match exacto en %s)",
            cust_id, field, value, json_key
        )
        return cust_id, created_on, updated_on

    return None


def _create_or_get_drellia_customer(
    *,
    phone_norm: str,
    external_id: str,
    identification_number: str,
    email: Optional[str],
    nombre: Optional[str],
) -> Optional[Tuple[str, datetime, datetime]]:

    api_key = _get_api_key_from_secret()
    phone_number = phone_norm or DEFAULT_PHONE

    if email and email.strip():
        email_use = email.strip()
    else:
        email_use = f"{phone_number}@placeholder.local"

    ident_use = identification_number.strip() if identification_number else external_id
    first_name, last_name = split_name(nombre)

    # 1) Buscar primero si YA existe en Drellia
    existing = _find_customer_http(
        external_id=external_id,
        email=email_use,
        identification_number=ident_use,
    )
    if existing:
        cust_id, created_on, updated_on = existing
        logger.info(
            "[DRELLIA][CUSTOMER] Ya existía customer id=%s phone=%s externalId=%s ident=%s",
            cust_id, phone_number, external_id, ident_use
        )
        return cust_id, created_on, updated_on

    # 2) No existe -> intentar crearlo con POST /v1/customers
    body: Dict[str, Any] = {
        "firstName":            first_name[:100],
        "lastName":             last_name[:100],
        "email":                email_use[:255],
        "externalId":           external_id[:100],
        "phoneNumber":          phone_number[:20],
        "identificationNumber": ident_use[:100],
    }

    url = f"{DRELLIA_BASE_URL.rstrip('/')}/v1/customers"
    headers = {
        "x-drellia-audit-api-key": api_key,
        "Content-Type": "application/json",
    }

    try:
        resp = _session.post(url, headers=headers, json=body, timeout=20)
    except Exception as e:
        logger.error("[DRELLIA][CUSTOMER] Excepción en POST /v1/customers: %s", e)
        return None

    # 200 / 201 -> creado OK
    if resp.status_code in (200, 201):
        try:
            data = resp.json()
        except Exception:
            logger.error("[DRELLIA][CUSTOMER] Respuesta no JSON: %s", resp.text[:400])
            return None

        cust = data.get("data") or data
        cust_id = cust.get("id")
        if not cust_id:
            logger.error("[DRELLIA][CUSTOMER] Respuesta sin id: %s", data)
            return None

        created_on = _iso_to_dt(cust.get("createdOn")) or now_utc()
        updated_on = _iso_to_dt(cust.get("updatedOn")) or created_on

        logger.info(
            "[DRELLIA][CUSTOMER] Creado/OK id=%s phone=%s externalId=%s ident=%s",
            cust_id, phone_number, external_id, ident_use
        )
        return cust_id, created_on, updated_on

    # 400 / 409 / 500 -> probable duplicado, validación o error interno.
    if resp.status_code in (400, 409, 500):
        logger.warning(
            "[DRELLIA][CUSTOMER] POST /v1/customers %s. body=%s",
            resp.status_code, resp.text[:400],
        )
        # Intentar recuperar por externalId/email/identificationNumber (por si se creó en otro momento)
        return _find_customer_http(
            external_id=external_id,
            email=email_use,
            identification_number=ident_use,
        )

    # Otros códigos -> error inesperado
    logger.error(
        "[DRELLIA][CUSTOMER] Error creando customer (%s): %s",
        resp.status_code, resp.text[:400]
    )
    return None


# --- ACCESO A drellia.customers ---

def _get_local_customer_by_phone(conn, phone_norm: str) -> Optional[Dict[str, Any]]:
    sql = """
    SELECT
      id_local,
      drellia_id,
      created_on,
      updated_on,
      deleted_on,
      created_by,
      updated_by,
      deleted_by,
      organization_id,
      identification_number,
      first_name,
      last_name,
      external_id,
      phone_number,
      email
    FROM drellia.customers
    WHERE phone_number = %s
    """
    cur = conn.cursor()
    try:
        cur.execute(sql, (phone_norm,))
        row = cur.fetchone()
    finally:
        cur.close()

    if not row:
        return None

    cols = [
        "id_local",
        "drellia_id",
        "created_on",
        "updated_on",
        "deleted_on",
        "created_by",
        "updated_by",
        "deleted_by",
        "organization_id",
        "identification_number",
        "first_name",
        "last_name",
        "external_id",
        "phone_number",
        "email",
    ]
    return dict(zip(cols, row))


def _upsert_local_customer(
    conn,
    *,
    phone_norm: str,
    drellia_id: Optional[str],
    created_on: Optional[datetime],
    updated_on: Optional[datetime],
    identification_number: Optional[str],
    first_name: str,
    last_name: str,
    external_id: str,
    email: str,
) -> None:

    created_on_use = created_on or now_utc()
    updated_on_use = updated_on or created_on_use

    # Caso 1: no tenemos drellia_id todavía -> registro PENDING (sin ON CONFLICT)
    if drellia_id is None:
        sql = """
        INSERT INTO drellia.customers (
          drellia_id,
          created_on,
          updated_on,
          deleted_on,
          created_by,
          updated_by,
          deleted_by,
          organization_id,
          identification_number,
          first_name,
          last_name,
          external_id,
          phone_number,
          email
        )
        VALUES (
          %s, %s, %s,
          NULL,
          NULL, NULL, NULL,
          NULL,
          %s, %s, %s,
          %s, %s, %s
        )
        """
        params = (
            None,
            created_on_use,
            updated_on_use,
            identification_number,
            first_name,
            last_name,
            external_id,
            phone_norm,
            email,
        )

    # Caso 2: ya tenemos drellia_id -> clave = drellia_id (canonical)
    else:
        sql = """
        INSERT INTO drellia.customers (
          drellia_id,
          created_on,
          updated_on,
          deleted_on,
          created_by,
          updated_by,
          deleted_by,
          organization_id,
          identification_number,
          first_name,
          last_name,
          external_id,
          phone_number,
          email
        )
        VALUES (
          %s, %s, %s,
          NULL,
          NULL, NULL, NULL,
          NULL,
          %s, %s, %s,
          %s, %s, %s
        )
        ON CONFLICT (drellia_id) DO UPDATE
        SET
          updated_on            = EXCLUDED.updated_on,
          identification_number = COALESCE(EXCLUDED.identification_number, drellia.customers.identification_number),
          first_name            = EXCLUDED.first_name,
          last_name             = EXCLUDED.last_name,
          external_id           = EXCLUDED.external_id,
          phone_number          = EXCLUDED.phone_number,
          email                 = EXCLUDED.email
        """
        params = (
            drellia_id,
            created_on_use,
            updated_on_use,
            identification_number,
            first_name,
            last_name,
            external_id,
            phone_norm,
            email,
        )

    cur = conn.cursor()
    try:
        cur.execute(sql, params)
        conn.commit()
    finally:
        cur.close()

    logger.info(
        "[CUSTOMER] Upsert local phone=%s drellia_id=%s externalId=%s email=%s",
        phone_norm, drellia_id, external_id, email
    )


# --- API PRINCIPAL: ensure_customer ---

def ensure_customer(
    conn,
    *,
    phone: Optional[str],
    identification_number: Optional[str],
    email: Optional[str],
    nombre: Optional[str],
) -> Optional[str]:

    # Normalizar teléfono para phoneNumber / clave local
    phone_norm = normalize_phone(phone)

    # Normalizar identificación (cedula) para externalId / identificationNumber
    if identification_number and identification_number.strip():
        ident_digits = "".join(ch for ch in str(identification_number) if ch.isdigit())
        ident_norm = ident_digits or DEFAULT_PHONE
    else:
        ident_norm = DEFAULT_PHONE

    external_id = ident_norm  # externalId estable basado en cedula

    # 1) Buscar local primero (cache) por phone_number
    local = _get_local_customer_by_phone(conn, phone_norm)
    if local and local.get("drellia_id"):
        logger.info(
            "[CUSTOMER] phone=%s ya sincronizado (drellia_id=%s)",
            phone_norm,
            local["drellia_id"]
        )
        return local["drellia_id"]

    # 2) Crear/obtener en Drellia
    cust_info = _create_or_get_drellia_customer(
        phone_norm=phone_norm,
        external_id=external_id,
        identification_number=ident_norm,
        email=email,
        nombre=nombre,
    )

    # 3) Si no se pudo, dejar registro local PENDING, sin drellia_id
    if not cust_info:
        first_name, last_name = split_name(nombre)
        if email and email.strip():
            email_use = email.strip()
        else:
            email_use = f"{phone_norm}@placeholder.local"

        _upsert_local_customer(
            conn,
            phone_norm=phone_norm,
            drellia_id=None,
            created_on=None,
            updated_on=None,
            identification_number=ident_norm,
            first_name=first_name,
            last_name=last_name,
            external_id=external_id,
            email=email_use,
        )
        logger.warning(
            "[CUSTOMER] No se pudo asegurar customer en Drellia para phone=%s; queda local en PENDING",
            phone_norm
        )
        return None

    # 4) Upsert local con drellia_id (SYNCED)
    drellia_id, created_on, updated_on = cust_info
    first_name, last_name = split_name(nombre)
    if email and email.strip():
        email_use = email.strip()
    else:
        email_use = f"{phone_norm}@placeholder.local"

    _upsert_local_customer(
        conn,
        phone_norm=phone_norm,
        drellia_id=drellia_id,
        created_on=created_on,
        updated_on=updated_on,
        identification_number=ident_norm,
        first_name=first_name,
        last_name=last_name,
        external_id=external_id,
        email=email_use,
    )

    return drellia_id