# customers_service.py
from __future__ import annotations

import re
import logging
from typing import Any, Dict, Optional

import db
import config

# Este módulo externo ya existe en tu sistema original
import manager_customer  

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)


# ========= HELPERS =========

def normalize_phone(phone: Optional[str]) -> Optional[str]:
    """
    Deja solo dígitos. Si viene None, retorno None.
    """
    if not phone:
        return None
    return re.sub(r"\D+", "", str(phone))


# ========= CUSTOMER LOOKUP + CREATE + UPDATE =========

def ensure_customer_for_job(
    conn,
    job: Dict[str, Any],
) -> Optional[str]:


    lote_id    = job["lote_id"]
    session_id = job["session_id"]

    telefono = job.get("telefono")
    email    = job.get("email")
    nombre   = job.get("nombre_cliente") or job.get("nombre_completo")

    # 1) Normalizar teléfono
    clean_phone = normalize_phone(telefono)
    id_number = clean_phone if clean_phone else "00000000000"

    # 2) Buscar en customers (lookup directo)
    existing_id = db.lookup_customer_by_identification(conn, id_number)
    if existing_id:
        customer_id = str(existing_id)
        db.update_customer_in_envio(conn, lote_id, session_id, customer_id)
        return customer_id
    try:
        created_id = manager_customer.ensure_customer(
            conn,
            phone=telefono,
            identification_number=id_number,
            email=email,
            nombre=nombre,
        )
    except Exception as e:
        logger.exception("[CUSTOMER] Error creando customer para session=%s lote=%s", session_id, lote_id)
        return None

    if not created_id:
        logger.warning("[CUSTOMER] No se pudo crear customer para session=%s", session_id)
        return None

    customer_id_str = str(created_id)

    # 4) Actualizamos envio_mensajes con el  customer_id
    try:
        db.update_customer_in_envio(conn, lote_id, session_id, customer_id_str)
    except Exception as e:
        logger.warning("[CUSTOMER] Error actualizando envio_mensajes con new customer_id=%s: %s",
                       customer_id_str, e)

    return customer_id_str
