# utils_email.py
from __future__ import annotations

import logging
import os
import smtplib
from email import encoders
from email.mime.base import MIMEBase
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from pathlib import Path
from typing import List, Optional, Sequence

logger = logging.getLogger(__name__)

if not logger.handlers:
    logging.basicConfig(level=logging.INFO)

logger.setLevel(logging.INFO)


# --- CONFIG SMTP POR ENV ---

SMTP_HOST = os.environ.get("SMTP_HOST")  # ej: "smtp.sendgrid.net" / "smtp.gmail.com"
SMTP_PORT = int(os.environ.get("SMTP_PORT", "587"))
SMTP_USER = os.environ.get("SMTP_USER")
SMTP_PASSWORD = os.environ.get("SMTP_PASSWORD")
SMTP_USE_TLS = os.environ.get("SMTP_USE_TLS", "true").lower() == "true"
SMTP_USE_SSL = os.environ.get("SMTP_USE_SSL", "false").lower() == "true"

EMAIL_FROM = os.environ.get("EMAIL_FROM")
EMAIL_TO = os.environ.get("EMAIL_TO")
EMAIL_DEFAULT_SUBJECT_PREFIX = os.environ.get("EMAIL_SUBJECT_PREFIX", "[Drellia] ")


def _parse_recipients(value: Optional[str]) -> List[str]:
    if not value:
        return []
    return [x.strip() for x in value.split(",") if x.strip()]


# --- CONSTRUCTOR DEL MENSAJE ---

def _build_message(
    subject: str,
    body_text: str,
    *,
    from_addr: str,
    to_addrs: Sequence[str],
    cc_addrs: Optional[Sequence[str]] = None,
    attachments: Optional[Sequence[str]] = None,
) -> MIMEMultipart:

    msg = MIMEMultipart()
    msg["From"] = from_addr
    msg["To"] = ", ".join(to_addrs)
    if cc_addrs:
        msg["Cc"] = ", ".join(cc_addrs)
    msg["Subject"] = subject

    # Cuerpo en texto plano
    msg.attach(MIMEText(body_text, "plain", "utf-8"))

    # Adjuntos
    for path in attachments or []:
        try:
            file_path = Path(path)
            if not file_path.is_file():
                logger.warning("[EMAIL] Adjuntar: archivo %s no existe, se omite.", path)
                continue

            with open(file_path, "rb") as f:
                part = MIMEBase("application", "octet-stream")
                part.set_payload(f.read())
            
            encoders.encode_base64(part)
            part.add_header(
                "Content-Disposition",
                f'attachment; filename="{file_path.name}"',
            )
            msg.attach(part)
            logger.info("[EMAIL] Archivo adjuntado: %s", file_path)

        except Exception as e:
            logger.warning("[EMAIL] Error adjuntando archivo %s: %s", path, e)

    return msg


# --- ENVÍO SMTP ---

def _send_smtp(
    msg: MIMEMultipart,
    from_addr: str,
    to_addrs: Sequence[str],
) -> None:

    if not SMTP_HOST:
        raise RuntimeError("SMTP_HOST no está configurado (variable de entorno SMTP_HOST).")

    all_recipients = list(to_addrs)
    cc_header = msg.get("Cc")

    if cc_header:
        all_recipients.extend([x.strip() for x in cc_header.split(",") if x.strip()])

    logger.info(
        "[EMAIL] Enviando correo a %s vía SMTP %s:%s TLS=%s SSL=%s",
        all_recipients, SMTP_HOST, SMTP_PORT, SMTP_USE_TLS, SMTP_USE_SSL
    )

    if SMTP_USE_SSL:
        server = smtplib.SMTP_SSL(SMTP_HOST, SMTP_PORT)
    else:
        server = smtplib.SMTP(SMTP_HOST, SMTP_PORT)

    try:
        server.ehlo()
        if SMTP_USE_TLS and not SMTP_USE_SSL:
            server.starttls()
            server.ehlo()

        if SMTP_USER and SMTP_PASSWORD:
            server.login(SMTP_USER, SMTP_PASSWORD)

        server.sendmail(from_addr, all_recipients, msg.as_string())
        logger.info("[EMAIL] Correo enviado correctamente.")

    finally:
        try:
            server.quit()
        except Exception:
            pass


# --- API PÚBLICA ---

def send_email(
    subject: str,
    body_text: str,
    *,
    to: Optional[Sequence[str]] = None,
    cc: Optional[Sequence[str]] = None,
    bcc: Optional[Sequence[str]] = None,
    attachments: Optional[Sequence[str]] = None,
    from_addr: Optional[str] = None,
) -> None:

    from_addr = from_addr or EMAIL_FROM

    if not from_addr:
        raise RuntimeError("EMAIL_FROM no está configurado (variable de entorno EMAIL_FROM).")

    # Destinatarios principales
    if to is None or len(to) == 0:
        to_addrs = _parse_recipients(EMAIL_TO)
    else:
        to_addrs = list(to)

    if not to_addrs:
        raise RuntimeError("No hay destinatarios (ni parámetro 'to' ni EMAIL_TO en entorno).")

    cc_addrs = list(cc) if cc else []
    bcc_addrs = list(bcc) if bcc else []

    # Prefijo de asunto opcional
    if EMAIL_DEFAULT_SUBJECT_PREFIX and not subject.startswith(EMAIL_DEFAULT_SUBJECT_PREFIX):
        subject_final = EMAIL_DEFAULT_SUBJECT_PREFIX + subject
    else:
        subject_final = subject

    msg = _build_message(
        subject=subject_final,
        body_text=body_text,
        from_addr=from_addr,
        to_addrs=to_addrs,
        cc_addrs=cc_addrs,
        attachments=attachments,
    )

    # BCC no se agrega a headers, solo se combina en la lista de envío
    send_to = list(to_addrs) + cc_addrs + bcc_addrs

    _send_smtp(msg, from_addr=from_addr, to_addrs=send_to)