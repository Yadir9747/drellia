# analisis_graficos.py
from __future__ import annotations

import os
from textwrap import wrap
from typing import Any, Dict, List

import matplotlib
import matplotlib.pyplot as plt
from matplotlib.backends.backend_pdf import PdfPages

# Configurar backend sin interfaz gráfica
matplotlib.use("Agg")


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

    # Extracción de datos
    basic = cuant_result.get("basic_stats", {}) or {}
    deptos = cuant_result.get("by_department", []) or []
    bot_agent_stats = cuant_result.get("bot_agent_stats", {}) or {}
    bot_agent_by_dept = cuant_result.get("bot_agent_by_department", []) or []
    summary_text = cuant_result.get("summary_text", "") or ""

    quali_text = quali_result.get("summary_text", "") or ""
    quali_snippet = quali_text[:2000] + ("..." if len(quali_text) > 2000 else "")

    with PdfPages(pdf_path) as pdf:
        # --- Página 1: Resumen texto ---
        fig1 = plt.figure(figsize=(8.27, 11.69))  # A4
        fig1.suptitle(f"Resumen Lote #{lote_num} (lote_id={lote_id})", fontsize=14, y=0.97)

        text_lines: List[str] = []
        text_lines.append("📊 Resumen cuantitativo")
        text_lines.append("")
        text_lines.extend(summary_text.splitlines())
        text_lines.append("")
        text_lines.append("🧠 Análisis cualitativo (extracto)")
        text_lines.append("")
        text_lines.extend(quali_snippet.splitlines())

        full_text = "\n".join(text_lines)

        ax1 = fig1.add_subplot(111)
        ax1.axis("off")

        wrapped = []
        for paragraph in full_text.split("\n"):
            wrapped.extend(wrap(paragraph, width=100) or [""])

        y = 0.95
        line_height = 0.018

        for line in wrapped:
            ax1.text(0.03, y, line, fontsize=8, va="top", ha="left")
            y -= line_height
            if y < 0.05:
                break

        pdf.savefig(fig1)
        plt.close(fig1)

        # --- Página 2: Conversaciones por depto ---
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

        # --- Página 3: BOT vs AGENTE por depto ---
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

        # --- Página 4: Tipos de flujo ---
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

    return pdf_path