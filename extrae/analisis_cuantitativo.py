# analisis_cuantitativo.py
from __future__ import annotations

import logging
from typing import Any, Dict, List, Optional

logger = logging.getLogger(__name__)

if not logger.handlers:
    logging.basicConfig(level=logging.INFO)

logger.setLevel(logging.INFO)

LOTES_TABLE_FQN = "drellia.lotes_conversaciones"


# --- HELPERS SQL ---

def _fetch_one_dict(cur, cols: List[str]) -> Optional[Dict[str, Any]]:
    row = cur.fetchone()
    if not row:
        return None
    return dict(zip(cols, row))


def _fetch_all_dicts(cur, cols: List[str]) -> List[Dict[str, Any]]:
    rows = cur.fetchall()
    return [dict(zip(cols, r)) for r in rows]


# --- MÉTRICAS BÁSICAS DEL LOTE ---

def compute_basic_stats(conn, lote_id: str) -> Dict[str, Any]:
    sql = f"""
    SELECT
      COUNT(*)::BIGINT                                                AS total_convs,
      AVG(mensajes_count)::FLOAT                                      AS avg_msgs_count,
      MIN(mensajes_count)::BIGINT                                     AS min_msgs_count,
      MAX(mensajes_count)::BIGINT                                     AS max_msgs_count,

      COUNT(DISTINCT telefono)                                        AS phones_distinct,
      SUM(CASE WHEN telefono IS NULL THEN 1 ELSE 0 END)::BIGINT       AS phones_null,

      COUNT(DISTINCT email)                                           AS emails_distinct,
      SUM(CASE WHEN email IS NULL THEN 1 ELSE 0 END)::BIGINT          AS emails_null,

      COUNT(DISTINCT cedula)                                          AS cedulas_distinct,
      SUM(CASE WHEN cedula IS NULL THEN 1 ELSE 0 END)::BIGINT         AS cedulas_null,

      AVG(mensajes_usuario)::FLOAT                                    AS avg_msgs_usuario,
      AVG(mensajes_sistema)::FLOAT                                    AS avg_msgs_sistema,

      SUM(CASE WHEN tiene_audio THEN 1 ELSE 0 END)::BIGINT            AS conversaciones_con_audio,
      AVG(CASE WHEN tiene_audio THEN 1.0 ELSE 0.0 END)::FLOAT         AS pct_con_audio,

      AVG(last_msg_ts_ms - first_msg_ts_ms)::FLOAT                    AS avg_duration_ms,
      MIN(last_msg_ts_ms - first_msg_ts_ms)::BIGINT                   AS min_duration_ms,
      MAX(last_msg_ts_ms - first_msg_ts_ms)::BIGINT                   AS max_duration_ms
    FROM {LOTES_TABLE_FQN}
    WHERE lote_id = %s
    """
    cur = conn.cursor()
    try:
        cur.execute(sql, (lote_id,))
        cols = [
            "total_convs",
            "avg_msgs_count",
            "min_msgs_count",
            "max_msgs_count",
            "phones_distinct",
            "phones_null",
            "emails_distinct",
            "emails_null",
            "cedulas_distinct",
            "cedulas_null",
            "avg_msgs_usuario",
            "avg_msgs_sistema",
            "conversaciones_con_audio",
            "pct_con_audio",
            "avg_duration_ms",
            "min_duration_ms",
            "max_duration_ms",
        ]
        row = _fetch_one_dict(cur, cols) or {}
    finally:
        cur.close()

    return row


def compute_by_department(conn, lote_id: str, top_n: int = 10) -> List[Dict[str, Any]]:
    sql = f"""
    SELECT
      COALESCE(cola_atencion, 'SIN_COLA') AS cola_atencion,
      COUNT(*)::BIGINT                    AS total,
      AVG(mensajes_count)::FLOAT          AS avg_msgs_count,
      SUM(CASE WHEN tiene_audio THEN 1 ELSE 0 END)::BIGINT AS con_audio
    FROM {LOTES_TABLE_FQN}
    WHERE lote_id = %s
    GROUP BY COALESCE(cola_atencion, 'SIN_COLA')
    ORDER BY total DESC
    LIMIT %s
    """
    cur = conn.cursor()
    try:
        cur.execute(sql, (lote_id, top_n))
        cols = ["cola_atencion", "total", "avg_msgs_count", "con_audio"]
        rows = _fetch_all_dicts(cur, cols)
    finally:
        cur.close()

    return rows


def compute_operator_email_coverage(conn, lote_id: str) -> Dict[str, Any]:
    # 1) Conteo de filas con/ sin operadores_emails_distintos
    sql1 = f"""
    SELECT
      SUM(CASE
            WHEN operadores_emails_distintos IS NULL
                 OR cardinality(operadores_emails_distintos) = 0
            THEN 1 ELSE 0 END)::BIGINT AS convs_sin_emails,
      SUM(CASE
            WHEN operadores_emails_distintos IS NOT NULL
                 AND cardinality(operadores_emails_distintos) > 0
            THEN 1 ELSE 0 END)::BIGINT AS convs_con_emails
    FROM {LOTES_TABLE_FQN}
    WHERE lote_id = %s
    """

    # 2) Distintos emails de operador en todo el lote
    sql2 = f"""
    SELECT
      COUNT(DISTINCT lower(email))::BIGINT AS total_emails_distintos
    FROM (
      SELECT unnest(operadores_emails_distintos) AS email
      FROM {LOTES_TABLE_FQN}
      WHERE lote_id = %s
        AND operadores_emails_distintos IS NOT NULL
    ) t
    WHERE email IS NOT NULL AND email <> '';
    """

    cur = conn.cursor()
    try:
        cur.execute(sql1, (lote_id,))
        row1 = cur.fetchone() or (0, 0)
        convs_sin_emails, convs_con_emails = row1

        cur.execute(sql2, (lote_id,))
        row2 = cur.fetchone() or (0,)
        total_emails_distintos = row2[0]
    finally:
        cur.close()

    return {
        "convs_sin_emails_operador": int(convs_sin_emails),
        "convs_con_emails_operador": int(convs_con_emails),
        "total_emails_operador_distintos": int(total_emails_distintos),
    }


def compute_phone_conversation_distribution(
    conn, lote_id: str, top_n: int = 10
) -> Dict[str, Any]:
    """
    Distribución de conversaciones por teléfono:
      - top_n teléfonos con más conversaciones
      - histograma simple de #conversaciones_por_tel
    """
    # Conversaciones por teléfono
    sql1 = f"""
    SELECT
      telefono,
      COUNT(*)::BIGINT AS convs
    FROM {LOTES_TABLE_FQN}
    WHERE lote_id = %s
    GROUP BY telefono
    ORDER BY convs DESC
    LIMIT %s
    """

    # Histograma: cuántos teléfonos tienen X conversaciones (1,2,3,...)
    sql2 = f"""
    WITH por_tel AS (
      SELECT
        telefono,
        COUNT(*)::BIGINT AS convs
      FROM {LOTES_TABLE_FQN}
      WHERE lote_id = %s
      GROUP BY telefono
    )
    SELECT
      convs            AS convs_por_telefono,
      COUNT(*)::BIGINT AS cantidad_telefonos
    FROM por_tel
    GROUP BY convs
    ORDER BY convs_por_telefono
    """

    cur = conn.cursor()
    try:
        cur.execute(sql1, (lote_id, top_n))
        cols1 = ["telefono", "convs"]
        top_phones = _fetch_all_dicts(cur, cols1)

        cur.execute(sql2, (lote_id,))
        cols2 = ["convs_por_telefono", "cantidad_telefonos"]
        hist = _fetch_all_dicts(cur, cols2)
    finally:
        cur.close()

    return {
        "top_telefonos": top_phones,
        "histograma_convs_por_telefono": hist,
    }


# --- MÉTRICAS BOT vs AGENTE / FLUJOS (APROXIMADAS) ---

def compute_bot_agent_stats(conn, lote_id: str) -> Dict[str, Any]:
    sql = f"""
    SELECT
      COUNT(*)::BIGINT AS total_convs,

      -- BOT: usamos mensajes_sistema
      SUM(mensajes_sistema)::BIGINT AS total_bot_msgs,
      AVG(mensajes_sistema)::FLOAT  AS avg_bot_msgs,

      -- AGENTE (aprox): total - user - sistema, forzando a >= 0
      SUM(GREATEST(mensajes_count - mensajes_usuario - mensajes_sistema, 0))::BIGINT AS total_agent_msgs,
      AVG(GREATEST(mensajes_count - mensajes_usuario - mensajes_sistema, 0))::FLOAT  AS avg_agent_msgs,

      -- Participación
      SUM(CASE WHEN mensajes_sistema > 0 THEN 1 ELSE 0 END)::BIGINT AS convs_con_bot,
      SUM(CASE
            WHEN operadores_emails_distintos IS NOT NULL
             AND cardinality(operadores_emails_distintos) > 0
            THEN 1 ELSE 0 END)::BIGINT AS convs_con_agente,

      -- Solo BOT / Solo AGENTE / Mixtas
      SUM(CASE
            WHEN mensajes_sistema > 0
             AND (operadores_emails_distintos IS NULL OR cardinality(operadores_emails_distintos) = 0)
            THEN 1 ELSE 0 END)::BIGINT AS convs_solo_bot,

      SUM(CASE
            WHEN mensajes_sistema = 0
             AND operadores_emails_distintos IS NOT NULL
             AND cardinality(operadores_emails_distintos) > 0
            THEN 1 ELSE 0 END)::BIGINT AS convs_solo_agente,

      SUM(CASE
            WHEN mensajes_sistema > 0
             AND operadores_emails_distintos IS NOT NULL
             AND cardinality(operadores_emails_distintos) > 0
            THEN 1 ELSE 0 END)::BIGINT AS convs_bot_y_agente

    FROM {LOTES_TABLE_FQN}
    WHERE lote_id = %s
    """
    cur = conn.cursor()
    try:
        cur.execute(sql, (lote_id,))
        cols = [
            "total_convs",
            "total_bot_msgs",
            "avg_bot_msgs",
            "total_agent_msgs",
            "avg_agent_msgs",
            "convs_con_bot",
            "convs_con_agente",
            "convs_solo_bot",
            "convs_solo_agente",
            "convs_bot_y_agente",
        ]
        row = _fetch_one_dict(cur, cols) or {}
    finally:
        cur.close()

    # Derivadas aproximadas
    convs_bot_y_agente = row.get("convs_bot_y_agente", 0) or 0

    # Aproximamos "BOT→AGENTE" como "BOT + AGENTE" (no sabemos el orden real)
    row["convs_bot_a_agente"] = convs_bot_y_agente

    # No tenemos tiempo exacto bot→agente
    row["avg_wait_bot_to_agent_ms"] = 0.0

    return row


def compute_bot_agent_by_department(
    conn, lote_id: str, top_n: int = 10
) -> List[Dict[str, Any]]:
    sql = f"""
    SELECT
      COALESCE(cola_atencion, 'SIN_COLA') AS cola_atencion,
      COUNT(*)::BIGINT                    AS total_convs,

      -- BOT presente
      SUM(CASE WHEN mensajes_sistema > 0 THEN 1 ELSE 0 END)::BIGINT AS convs_con_bot,

      -- AGENTE presente
      SUM(CASE
            WHEN operadores_emails_distintos IS NOT NULL
             AND cardinality(operadores_emails_distintos) > 0
            THEN 1 ELSE 0 END)::BIGINT AS convs_con_agente,

      -- Conversaciones donde hay BOT y AGENTE
      SUM(CASE
            WHEN mensajes_sistema > 0
             AND operadores_emails_distintos IS NOT NULL
             AND cardinality(operadores_emails_distintos) > 0
            THEN 1 ELSE 0 END)::BIGINT AS convs_bot_y_agente,

      AVG(mensajes_sistema)::FLOAT AS avg_bot_msgs,
      AVG(GREATEST(mensajes_count - mensajes_usuario - mensajes_sistema, 0))::FLOAT AS avg_agent_msgs

    FROM {LOTES_TABLE_FQN}
    WHERE lote_id = %s
    GROUP BY COALESCE(cola_atencion, 'SIN_COLA')
    ORDER BY total_convs DESC
    LIMIT %s
    """
    cur = conn.cursor()
    try:
        cur.execute(sql, (lote_id, top_n))
        cols = [
            "cola_atencion",
            "total_convs",
            "convs_con_bot",
            "convs_con_agente",
            "convs_bot_y_agente",
            "avg_bot_msgs",
            "avg_agent_msgs",
        ]
        rows = _fetch_all_dicts(cur, cols)
    finally:
        cur.close()

    # Completar campos derivados
    for r in rows:
        convs_bot_y_agente = r.get("convs_bot_y_agente", 0) or 0
        r["convs_bot_a_agente"] = convs_bot_y_agente
        r["avg_wait_bot_to_agent_ms"] = 0.0

    return rows


# --- BUILDER DE RESUMEN EN TEXTO ---

def build_text_summary(
    lote_id: str,
    lote_num: int,
    basic: Dict[str, Any],
    deptos: List[Dict[str, Any]],
    op_emails: Dict[str, Any],
    bot_agent_stats: Dict[str, Any],
    bot_agent_by_dept: List[Dict[str, Any]],
) -> str:
    # ---------- Básicos ----------
    total = basic.get("total_convs", 0) or 0
    phones_dist = basic.get("phones_distinct", 0) or 0
    phones_null = basic.get("phones_null", 0) or 0
    avg_msgs = basic.get("avg_msgs_count") or 0.0
    avg_user = basic.get("avg_msgs_usuario") or 0.0
    avg_sys = basic.get("avg_msgs_sistema") or 0.0

    convs_con_audio = basic.get("conversaciones_con_audio", 0) or 0
    pct_audio = (basic.get("pct_con_audio") or 0.0) * 100.0

    emails_null = basic.get("emails_null", 0) or 0
    emails_dist = basic.get("emails_distinct", 0) or 0
    convs_sin_emails = op_emails.get("convs_sin_emails_operador", 0) or 0
    convs_con_emails = op_emails.get("convs_con_emails_operador", 0) or 0
    total_emails_oper = op_emails.get("total_emails_operador_distintos", 0) or 0

    avg_dur = (basic.get("avg_duration_ms") or 0.0) / 1000.0
    min_dur = (basic.get("min_duration_ms") or 0.0) / 1000.0
    max_dur = (basic.get("max_duration_ms") or 0.0) / 1000.0

    # ---------- Bot vs Agente ----------
    total_bot_msgs = bot_agent_stats.get("total_bot_msgs", 0) or 0
    total_agent_msgs = bot_agent_stats.get("total_agent_msgs", 0) or 0
    avg_bot_msgs = bot_agent_stats.get("avg_bot_msgs", 0.0) or 0.0
    avg_agent_msgs = bot_agent_stats.get("avg_agent_msgs", 0.0) or 0.0

    convs_solo_bot = bot_agent_stats.get("convs_solo_bot", 0) or 0
    convs_solo_agente = bot_agent_stats.get("convs_solo_agente", 0) or 0
    convs_bot_y_agente = bot_agent_stats.get("convs_bot_y_agente", 0) or 0
    convs_bot_a_agente = bot_agent_stats.get("convs_bot_a_agente", 0) or 0

    avg_wait_bot_to_agent_ms = (
        bot_agent_stats.get("avg_wait_bot_to_agent_ms", 0.0) or 0.0
    )
    avg_wait_bot_to_agent_s = (
        avg_wait_bot_to_agent_ms / 1000.0 if avg_wait_bot_to_agent_ms else 0.0
    )

    # porcentajes seguros
    def pct(part: int, whole: int) -> float:
        if not whole:
            return 0.0
        return (float(part) / float(whole)) * 100.0

    pct_solo_bot = pct(convs_solo_bot, total)
    pct_solo_agente = pct(convs_solo_agente, total)
    pct_bot_y_agente = pct(convs_bot_y_agente, total)
    pct_bot_a_agente = pct(convs_bot_a_agente, total)

    # ---------- Armado del texto ----------
    lineas: List[str] = []
    lineas.append(f"Resumen cuantitativo del lote #{lote_num} (lote_id={lote_id})")
    lineas.append("")

    # Conversaciones
    lineas.append("📌 Conversaciones")
    lineas.append(f"- Total de conversaciones: {total}")
    lineas.append(f"- Teléfonos distintos: {phones_dist} (NULL: {phones_null})")
    lineas.append(f"- Emails distintos: {emails_dist} (NULL: {emails_null})")
    lineas.append("")

    # Mensajes
    lineas.append("💬 Mensajes")
    lineas.append(f"- Mensajes promedio por conversación: {avg_msgs:.2f}")
    lineas.append(f"- Mensajes promedio del CLIENTE: {avg_user:.2f}")
    lineas.append(f"- Mensajes promedio del BOT/SISTEMA: {avg_sys:.2f}")
    lineas.append("")

    # Bot vs Agente
    lineas.append("🤖 vs 👤 Bot / Agente (aproximado)")
    lineas.append(
        f"- Mensajes totales del BOT: {total_bot_msgs} (prom: {avg_bot_msgs:.2f} por conv.)"
    )
    lineas.append(
        f"- Mensajes totales de AGENTES: {total_agent_msgs} (prom: {avg_agent_msgs:.2f} por conv.)"
    )
    lineas.append(f"- Conversaciones SOLO BOT: {convs_solo_bot} ({pct_solo_bot:.1f}%)")
    lineas.append(
        f"- Conversaciones SOLO AGENTE: {convs_solo_agente} ({pct_solo_agente:.1f}%)"
    )
    lineas.append(
        f"- Conversaciones mixtas BOT + AGENTE: {convs_bot_y_agente} ({pct_bot_y_agente:.1f}%)"
    )
    lineas.append(
        f"- Conversaciones con flujo BOT → AGENTE (aprox): {convs_bot_a_agente} ({pct_bot_a_agente:.1f}%)"
    )
    if avg_wait_bot_to_agent_s:
        lineas.append(
            f"- Espera promedio bot → agente: ~{avg_wait_bot_to_agent_s:.1f} segundos (aprox)"
        )
    else:
        lineas.append(
            "- Espera promedio bot → agente: no disponible con el esquema actual"
        )
    lineas.append("")

    # Audio
    lineas.append("🎧 Audio")
    lineas.append(f"- Conversaciones con audio: {convs_con_audio} ({pct_audio:.1f}%)")
    lineas.append("")

    # Operadores / Emails
    lineas.append("👥 Operadores (emails en las conversaciones)")
    lineas.append(f"- Conversaciones SIN emails de operador: {convs_sin_emails}")
    lineas.append(f"- Conversaciones CON algún email de operador: {convs_con_emails}")
    lineas.append(f"- Emails de operador distintos en el lote: {total_emails_oper}")
    lineas.append("")

    # Duraciones
    lineas.append("⏱ Duración de las conversaciones (aprox., en segundos)")
    lineas.append(f"- Promedio: {avg_dur:.1f}s")
    lineas.append(f"- Mínimo:   {min_dur:.1f}s")
    lineas.append(f"- Máximo:   {max_dur:.1f}s")
    lineas.append("")

    # Departamentos simples (mensajes/audio)
    if deptos:
        lineas.append("📂 Distribución por cola/departamento (top):")
        for d in deptos:
            cola = d["cola_atencion"]
            total_c = d["total"]
            avg_m = d["avg_msgs_count"] or 0.0
            con_a = d["con_audio"] or 0
            lineas.append(
                f"- {cola}: {total_c} convs, {avg_m:.1f} msgs/prom, {con_a} con audio"
            )
    else:
        lineas.append("📂 Distribución por cola/departamento: sin datos")
    lineas.append("")

    # Departamentos: Bot vs Agente
    if bot_agent_by_dept:
        lineas.append("🏷 Distribución Bot / Agente por departamento (top, aproximado):")
        for d in bot_agent_by_dept:
            cola = d["cola_atencion"]
            total_c = d["total_convs"]
            convs_con_bot = d["convs_con_bot"] or 0
            convs_con_agente = d["convs_con_agente"] or 0
            convs_bot_a_agente_d = d["convs_bot_a_agente"] or 0
            pct_bot = pct(convs_con_bot, total_c)
            pct_agent = pct(convs_con_agente, total_c)
            pct_bot2agent = pct(convs_bot_a_agente_d, total_c)
            avg_bot_d = d["avg_bot_msgs"] or 0.0
            avg_agent_d = d["avg_agent_msgs"] or 0.0
            lineas.append(
                f"- {cola}: {total_c} convs | BOT en {pct_bot:.1f}%, AGENTE en {pct_agent:.1f}%, "
                f"BOT→AGENTE (aprox) en {pct_bot2agent:.1f}% | msgs BOT: {avg_bot_d:.1f}, msgs AGENTE: {avg_agent_d:.1f}"
            )
    else:
        lineas.append("🏷 Distribución Bot / Agente por departamento: sin datos")
    
    return "\n".join(lineas)


# --- ORQUESTADOR DEL MÓDULO ---

def run_analisis_cuantitativo(
    conn,
    *,
    lote_id: str,
    lote_num: int,
    top_deptos: int = 10,
) -> Dict[str, Any]:

    basic = compute_basic_stats(conn, lote_id)
    deptos = compute_by_department(conn, lote_id, top_n=top_deptos)
    op_emails = compute_operator_email_coverage(conn, lote_id)
    phone_dist = compute_phone_conversation_distribution(conn, lote_id, top_n=10)
    bot_agent_stats = compute_bot_agent_stats(conn, lote_id)
    bot_agent_by_dept = compute_bot_agent_by_department(conn, lote_id, top_n=top_deptos)

    summary_text = build_text_summary(
        lote_id=lote_id,
        lote_num=lote_num,
        basic=basic,
        deptos=deptos,
        op_emails=op_emails,
        bot_agent_stats=bot_agent_stats,
        bot_agent_by_dept=bot_agent_by_dept,
    )

    return {
        "basic_stats": basic,
        "by_department": deptos,
        "operator_email_coverage": op_emails,
        "phone_distribution": phone_dist,
        "bot_agent_stats": bot_agent_stats,
        "bot_agent_by_department": bot_agent_by_dept,
        "summary_text": summary_text,
    }