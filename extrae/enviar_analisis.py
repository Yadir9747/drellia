# enviar_analisis.py
from __future__ import annotations

import json
import logging
import os
from textwrap import wrap
from typing import Any, Dict, List, Optional

import matplotlib
import matplotlib.pyplot as plt
from matplotlib.backends.backend_pdf import PdfPages

from google.cloud import secretmanager
from google.cloud.sql.connector import Connector

import analisis_cuantitativo
import analisis_cualitativo
import utils_email

# Para generar PDF con gráficas (backend sin interfaz gráfica)
matplotlib.use("Agg")


# --- CONFIG ---

PROJECT_ID = os.environ.get("PROJECT_ID", "data-323821")

PG_INSTANCE_CONN_NAME = os.environ["PG_INSTANCE_DRELLIA"]
PG_DB_SECRET_NAME = os.environ.get("PG_DB_SECRET_NAME", "PG_DB_SECRET_DRELLIA")
PG_DB_NAME = os.environ.get("PG_DB_NAME", "drellia")
PG_DB_SCHEMA = os.environ.get("PG_DB_SCHEMA", "drellia")

# Para Vertex AI (Gemini) – opcional
VERTEX_PROJECT_ID = os.environ.get("VERTEX_PROJECT_ID", PROJECT_ID)
VERTEX_LOCATION = os.environ.get("VERTEX_LOCATION", "us-central1")
VERTEX_MODEL_NAME = os.environ.get("VERTEX_MODEL_NAME", "gemini-2.5-pro")

# Cantidad máxima de conversaciones a usar para el análisis cualitativo
QUALI_MAX_CONVS = int(os.environ.get("QUALI_MAX_CONVS", "1000"))

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

_connector: Optional[Connector] = None


# --- HELPERS CONEXIÓN ---

def get_secret_text(secret_name: str) -> str:
    sm = secretmanager.SecretManagerServiceClient()
    full_name = f"projects/{PROJECT_ID}/secrets/{secret_name}/versions/latest"
    resp = sm.access_secret_version(request={"name": full_name})
    return resp.payload.data.decode("utf-8")


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


# --- HELPERS PARA CONVERSACIONES DE MUESTRA (QUALI) ---

def fetch_sample_conversations(
    conn,
    lote_id: str,
    max_convs: int = QUALI_MAX_CONVS,
) -> List[Dict[str, Any]]:
    sql = f"""
    SELECT
      session_id,
      telefono,
      cedula,
      email,
      nombre_cliente,
      nombre_completo,
      mensajes
    FROM {PG_DB_SCHEMA}.lotes_conversaciones
    WHERE lote_id = %s
    ORDER BY mensajes_count DESC
    LIMIT %s
    """
    cur = conn.cursor()
    try:
        cur.execute(sql, (lote_id, max_convs))
        rows = cur.fetchall()
    finally:
        cur.close()

    cols = [
        "session_id",
        "telefono",
        "cedula",
        "email",
        "nombre_cliente",
        "nombre_completo",
        "mensajes",
    ]
    return [dict(zip(cols, r)) for r in rows]


# --- BUILD PDF REPORT (matplotlib + texto) ---

def build_pdf_report(
    lote_id: str,
    lote_num: int,
    cuant_result: Dict[str, Any],
    quali_result: Dict[str, Any],
    pdf_dir: str = "/tmp",
) -> str:
    os.makedirs(pdf_dir, exist_ok=True)

    safe_lote_id = lote_id.replace("-", "")
    pdf_name = f"reporte_lote_{lote_num}_{safe_lote_id}.pdf"
    pdf_path = os.path.join(pdf_dir, pdf_name)

    basic = cuant_result.get("basic_stats", {}) or {}
    deptos = cuant_result.get("by_department", []) or []
    bot_agent_stats = cuant_result.get("bot_agent_stats", {}) or {}
    bot_agent_by_dept = cuant_result.get("bot_agent_by_department", []) or []
    summary_text = cuant_result.get("summary_text", "") or ""

    quali_text = quali_result.get("summary_text", "") or ""
    quali_snippet = quali_text[:2000] + ("..." if len(quali_text) > 2000 else "")

    logger.info("[PDF] Generando reporte PDF en %s", pdf_path)

    with PdfPages(pdf_path) as pdf:
        # Página 1: Resumen texto
        fig1 = plt.figure(figsize=(8.27, 11.69))  # A4 en pulgadas aprox
        fig1.suptitle(f"Resumen Lote #{lote_num} (lote_id={lote_id})", fontsize=14, y=0.97)

        # Construimos un texto combinado
        text_lines: List[str] = []
        text_lines.append("📊 Resumen cuantitativo")
        text_lines.append("")
        for line in summary_text.splitlines():
            text_lines.append(line)
        text_lines.append("")
        text_lines.append("🧠 Análisis cualitativo (extracto)")
        text_lines.append("")
        for line in quali_snippet.splitlines():
            text_lines.append(line)

        full_text = "\n".join(text_lines)

        # Dibujamos el texto en la figura (sin ejes)
        ax1 = fig1.add_subplot(111)
        ax1.axis("off")

        # Wrap automático para que quepa en la página
        wrapped = []
        for paragraph in full_text.split("\n"):
            wrapped.extend(wrap(paragraph, width=100) or [""])

        y = 0.95
        line_height = 0.018

        for line in wrapped:
            ax1.text(0.03, y, line, fontsize=8, va="top", ha="left")
            y -= line_height
            if y < 0.05:
                # si se llena la página cortamos (es solo una portada/resumen)
                break

        pdf.savefig(fig1)
        plt.close(fig1)

        # Página 2: Distribución por departamento (total convs)
        if deptos:
            fig2, ax2 = plt.subplots(figsize=(8.27, 5))
            labels = [d["cola_atencion"] for d in deptos]
            totals = [int(d["total"]) for d in deptos]
            ax2.bar(range(len(labels)), totals)
            ax2.set_title("Conversaciones por departamento")
            ax2.set_xlabel("Departamento")
            ax2.set_ylabel("Total conversaciones")
            ax2.set_xticks(range(len(labels)))
            ax2.set_xticklabels(labels, rotation=45, ha="right", fontsize=8)
            fig2.tight_layout()
            pdf.savefig(fig2)
            plt.close(fig2)

        # Página 3: Bot vs Agente por departamento (si hay datos)
        if bot_agent_by_dept:
            fig3, ax3 = plt.subplots(figsize=(8.27, 5))
            labels = [d["cola_atencion"] for d in bot_agent_by_dept]
            convs_bot = [int(d["convs_con_bot"] or 0) for d in bot_agent_by_dept]
            convs_agente = [int(d["convs_con_agente"] or 0) for d in bot_agent_by_dept]
            x = range(len(labels))

            width = 0.35
            ax3.bar([i - width / 2 for i in x], convs_bot, width=width, label="Con BOT")
            ax3.bar([i + width / 2 for i in x], convs_agente, width=width, label="Con AGENTE")

            ax3.set_title("BOT vs AGENTE por departamento")
            ax3.set_xlabel("Departamento")
            ax3.set_ylabel("Cantidad de conversaciones")
            ax3.set_xticks(list(x))
            ax3.set_xticklabels(labels, rotation=45, ha="right", fontsize=8)
            ax3.legend(fontsize=8)
            fig3.tight_layout()
            pdf.savefig(fig3)
            plt.close(fig3)

        # Página 4: Tipos de flujo (BOT_ONLY / AGENT_ONLY / mixtas)
        if bot_agent_stats:
            fig4, ax4 = plt.subplots(figsize=(8.27, 5))
            convs_solo_bot = int(bot_agent_stats.get("convs_solo_bot") or 0)
            convs_solo_agente = int(bot_agent_stats.get("convs_solo_agente") or 0)
            convs_bot_y_agente = int(bot_agent_stats.get("convs_bot_y_agente") or 0)
            convs_bot_a_agente = int(bot_agent_stats.get("convs_bot_a_agente") or 0)

            labels_flow = [
                "Solo BOT",
                "Solo AGENTE",
                "BOT + AGENTE",
                "BOT→AGENTE",
            ]
            values_flow = [
                convs_solo_bot,
                convs_solo_agente,
                convs_bot_y_agente,
                convs_bot_a_agente,
            ]

            ax4.bar(range(len(labels_flow)), values_flow)
            ax4.set_title("Tipos de flujo conversacional")
            ax4.set_xlabel("Tipo de flujo")
            ax4.set_ylabel("Cantidad de conversaciones")
            ax4.set_xticks(range(len(labels_flow)))
            ax4.set_xticklabels(labels_flow, rotation=0, ha="center", fontsize=8)

            fig4.tight_layout()
            pdf.savefig(fig4)
            plt.close(fig4)

    logger.info("[PDF] Reporte PDF generado en %s", pdf_path)
    return pdf_path


# --- ORQUESTADOR PRINCIPAL ---

def run_analisis(
    lote_id: str,
    lote_num: int,
    window_hours: int,
    stats: Dict[str, Any],
) -> Dict[str, Any]:
    logger.info(
        "[ANALISIS] Iniciando run_analisis para lote_id=%s lote_num=%d window_hours=%d",
        lote_id, lote_num, window_hours
    )

    cuant_result: Dict[str, Any]
    quali_result: Dict[str, Any]
    convs_sample: List[Dict[str, Any]] = []

    # 1) Conexión a Postgres y análisis cuantitativo + muestra cualitativa
    conn = get_pg_conn()
    try:
        cuant_result = analisis_cuantitativo.run_analisis_cuantitativo(
            conn,
            lote_id=lote_id,
            lote_num=lote_num,
        )
        convs_sample = fetch_sample_conversations(conn, lote_id, max_convs=QUALI_MAX_CONVS)
    finally:
        try:
            conn.close()
        except Exception:
            pass

    # 2) Análisis cualitativo (Gemini u otro modelo)
    quali_result = analisis_cualitativo.run_analisis_cualitativo(
        convs_sample,
        lote_id=lote_id,
        lote_num=lote_num,
        project_id=VERTEX_PROJECT_ID,
        location=VERTEX_LOCATION,
        model_name=VERTEX_MODEL_NAME,
        base_dir="/tmp",
    )

    # 3) Construir cuerpo del email (mezcla cuantitativo + extracto cualitativo)
    cuant_text = cuant_result.get("summary_text", "").strip()
    quali_text = quali_result.get("summary_text", "").strip()

    # Extracto corto del cualitativo para el cuerpo
    quali_snippet = quali_text[:1500] + ("..." if len(quali_text) > 1500 else "")

    body_lines: List[str] = []
    body_lines.append(f"Resumen del lote #{lote_num}")
    body_lines.append(f"lote_id: {lote_id}")
    body_lines.append(f"Ventana de extracción: últimas {window_hours} horas")
    body_lines.append("")
    body_lines.append("📊 ESTADÍSTICAS GENERALES DEL EXTRACT:")
    body_lines.append(f"- Filas extraídas desde Botmaker: {stats.get('rows_extracted')}")
    body_lines.append(f"- Filas insertadas en lotes_conversaciones: {stats.get('rows_inserted')}")
    body_lines.append("")
    body_lines.append("📈 ANÁLISIS CUANTITATIVO DEL LOTE:")
    body_lines.append(cuant_text or "(sin datos cuantitativos)")
    body_lines.append("")
    body_lines.append("🧠 ANÁLISIS CUALITATIVO (extracto):")
    body_lines.append(quali_snippet or "(sin análisis cualitativo)")
    body_lines.append("")
    body_lines.append("Archivos adjuntos:")

    if quali_result.get("file_path"):
        body_lines.append(f"- Resumen cualitativo completo (texto): {os.path.basename(quali_result['file_path'])}")
    else:
        body_lines.append("- (no se generó archivo cualitativo .txt)")

    body_lines.append("- Reporte PDF con gráficas y resumen ampliado")
    body_text = "\n".join(body_lines)

    # 4) Adjuntos: resumen cualitativo + PDF
    attachments: List[str] = []

    if quali_result.get("file_path"):
        attachments.append(quali_result["file_path"])

    pdf_path = build_pdf_report(
        lote_id=lote_id,
        lote_num=lote_num,
        cuant_result=cuant_result,
        quali_result=quali_result,
        pdf_dir="/tmp",
    )
    attachments.append(pdf_path)

    # 5) Enviar email
    subject = f"Resumen lote #{lote_num} (lote_id={lote_id})"
    try:
        utils_email.send_email(
            subject=subject,
            body_text=body_text,
            attachments=attachments,
        )
        email_status = "SENT"
    except Exception as e:
        logger.exception("[ANALISIS] Error enviando email de resumen para lote_id=%s", lote_id)
        email_status = f"ERROR_SENDING_EMAIL: {e}"

    # 6) Devolver resultado estructurado
    result: Dict[str, Any] = {
        "status": "OK",
        "lote_id": lote_id,
        "lote_num": lote_num,
        "window_hours": window_hours,
        "extract_stats": stats,
        "cuantitativo": cuant_result,
        "cualitativo": {
            "summary_text": quali_text,
            "file_path": quali_result.get("file_path"),
            "n_convs_input": quali_result.get("n_convs_input"),
            "n_convs_used": quali_result.get("n_convs_used"),
        },
        "pdf_path": pdf_path,
        "email_status": email_status,
    }

    logger.info(
        "[ANALISIS] Finalizado run_analisis lote_id=%s lote_num=%d email_status=%s",
        lote_id, lote_num, email_status
    )
    return result