# analisis_cualitativo.py
from __future__ import annotations

import ast
import json
import logging
import os
import re
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional

# Imports opcionales para Vertex AI / Gemini
try:
    import vertexai
    from vertexai.generative_models import GenerativeModel, GenerationConfig
    _HAS_VERTEX = True
except ImportError:
    _HAS_VERTEX = False


logger = logging.getLogger(__name__)

if not logger.handlers:
    logging.basicConfig(level=logging.INFO)

logger.setLevel(logging.INFO)


# --- CONFIG / CONSTANTES ---

MAX_CONVS_IN_PROMPT = int(os.environ.get("QUALI_MAX_CONVS", "1000"))
MAX_MSGS_PER_CONV = int(os.environ.get("QUALI_MAX_MSGS_PER_CONV", "1000"))
DEFAULT_VERTEX_LOCATION = os.environ.get("VERTEX_LOCATION", "us-central1")
DEFAULT_VERTEX_MODEL = os.environ.get("VERTEX_MODEL_NAME", "gemini-2.5-pro")
DEFAULT_VERTEX_PROJECT = os.environ.get("VERTEX_PROJECT_ID")


# --- HELPERS GENERALES ---

def now_utc() -> datetime:
    return datetime.now(timezone.utc)


# --- NORMALIZACIÓN DE MENSAJES ---

def _normalize_mensajes(mensajes_raw: Any) -> List[Dict[str, Any]]:
    if mensajes_raw is None:
        return []

    # 1) Ya viene como lista de dicts
    if isinstance(mensajes_raw, list):
        msgs = [m for m in mensajes_raw if isinstance(m, dict)]
        return msgs

    # 2) Si viene como string, intentamos varias estrategias
    if isinstance(mensajes_raw, str):
        s = mensajes_raw.strip()
        if not s:
            return []

        # 2.1 Intentar JSON real (por si en el futuro guardas JSON)
        try:
            val = json.loads(s)
        except Exception:
            val = None

        if isinstance(val, list):
            msgs = [m for m in val if isinstance(m, dict)]
            if msgs:
                return msgs

        if isinstance(val, dict):
            return [val]

        if isinstance(val, str):
            # Intentamos parsear este string interno como dump Python
            inner_parsed = _try_parse_python_dump_string(val.strip())
            if inner_parsed is not None:
                return inner_parsed

        # 2.2 Formato Python con datetime + saltos de línea entre dicts
        #     ej: "[{'message_time': datetime.datetime(...), ...}\n {...}\n {...}]"
        if "datetime.datetime(" in s and "mensaje'" in s:
            inner = s
            # Quitamos corchetes exteriores si los hay
            if inner.startswith("[") and inner.endswith("]"):
                inner = inner[1:-1].strip()

            raw_chunks = inner.split("\n")
            out: List[Dict[str, Any]] = []

            for ch in raw_chunks:
                ch_s = ch.strip()
                if not ch_s:
                    continue

                # Forzar a que parezca un dict bien cerrado
                if not ch_s.startswith("{"):
                    idx = ch_s.find("{")
                    if idx != -1:
                        ch_s = ch_s[idx:]

                if not ch_s.endswith("}"):
                    last = ch_s.rfind("}")
                    if last != -1:
                        ch_s = ch_s[: last + 1]

                if not (ch_s.startswith("{") and ch_s.endswith("}")):
                    continue

                # Limpiar datetime.datetime(...) y tzinfo=<UTC>
                ch_clean = re.sub(r"datetime\.datetime\([^)]*\)", "None", ch_s)
                ch_clean = ch_clean.replace("tzinfo=<UTC>", "tzinfo=None")

                try:
                    obj = ast.literal_eval(ch_clean)
                    if isinstance(obj, dict):
                        out.append(obj)
                    else:
                        logger.warning(
                            "[QUALI] Bloque de mensajes no es dict tras literal_eval. type=%s",
                            type(obj),
                        )
                except Exception as e:
                    logger.warning(
                        "[QUALI] No se pudo parsear bloque de mensajes con literal_eval: %s\nBloque (recortado): %s",
                        e,
                        ch_s[:200].replace("\n", "\\n"),
                    )

            if out:
                return out

        # 2.3 Fallback extremo: no pudimos parsear estructurado, pero NO devolvemos vacío.
        logger.warning(
            "[QUALI] `mensajes` string no parseable de forma estructurada. "
            "Se enviará como un único mensaje crudo. len=%s inicio=%s...",
            len(s),
            s[:120].replace("\n", "\\n"),
        )
        return [
            {
                "us_origen": "desconocido",
                "mensaje": s,
            }
        ]

    # 3) Otros tipos raros
    logger.warning("[QUALI] Tipo no soportado para `mensajes`: %s", type(mensajes_raw))
    return []


def _try_parse_python_dump_string(s: str) -> Optional[List[Dict[str, Any]]]:
    if "datetime.datetime" not in s or "'mensaje':" not in s:
        return None
    try:
        # Reemplazamos cada datetime.datetime(...) por None para que literal_eval no explote
        cleaned = re.sub(r"datetime\.datetime\([^)]*\)", "None", s)
        cleaned = cleaned.replace("tzinfo=<UTC>", "tzinfo=None")
        val = ast.literal_eval(cleaned)

        if isinstance(val, list):
            return [m for m in val if isinstance(m, dict)]
        if isinstance(val, dict):
            return [val]
        return None
    except Exception as e:
        logger.warning("[QUALI] Error parseando dump Python con literal_eval: %s", e)
        return None


# --- META POR CONVERSACIÓN (FLOW, BOT/AGENTE, ETC.) ---

def _extract_conversation_meta(mensajes_list: List[Dict[str, Any]]) -> Dict[str, Any]:
    has_bot = False
    has_agent = False
    first_bot_idx: Optional[int] = None
    first_agent_idx: Optional[int] = None
    user_requested_agent = False
    bot_derived_to_agent = False
    departamentos: List[str] = []

    # Palabras clave sencillas
    user_agent_keywords = [
        "hablar con un agente",
        "hablar con agente",
        "hablar con un asesor",
        "hablar con asesor",
        "quiero hablar con un agente",
        "quiero hablar con un asesor",
        "quiero un agente",
        "quiero un asesor",
        "agente humano",
        "operador humano",
    ]

    bot_deriva_keywords = [
        "derivarte con un asesor",
        "derivarte con un agente",
        "derivarte con un representante",
        "derivarte con un operador",
        "te derivo con un asesor",
        "te derivo con un agente",
        "te derivo con un operador",
        "te voy a derivar",
        "estoy derivándote con un asesor",
        "pronto un agente se contactará contigo",
        "serás atendido por un operador",
        "en este momento comienzas a ser atendido por un representante",
    ]

    for idx, m in enumerate(mensajes_list):
        rol = (m.get("us_origen") or "").lower()
        text = str(m.get("mensaje") or "").lower()
        depto = m.get("departamento")

        if depto:
            departamentos.append(str(depto))

        # BOT
        if rol in ("bot", "flow", "ivr", "system"):
            has_bot = True
            if first_bot_idx is None:
                first_bot_idx = idx
            
            # Bot derivando a agente
            if any(kw in text for kw in bot_deriva_keywords):
                bot_derived_to_agent = True

        # AGENTE
        if rol in ("operator", "agent", "agente", "supervisor"):
            has_agent = True
            if first_agent_idx is None:
                first_agent_idx = idx

        # USER pidiendo agente
        if rol in ("user", "cliente", "client", "customer"):
            if any(kw in text for kw in user_agent_keywords):
                user_requested_agent = True

    # Determinar tipo de flujo
    if has_bot and not has_agent:
        flow_type = "BOT_ONLY"
    elif has_agent and not has_bot:
        flow_type = "AGENT_ONLY"
    elif has_bot and has_agent:
        # Si el bot aparece antes que el agente: BOT_TO_AGENT
        if (
            first_bot_idx is not None
            and first_agent_idx is not None
            and first_bot_idx < first_agent_idx
        ):
            flow_type = "BOT_TO_AGENT"
        else:
            flow_type = "OTHER"
    else:
        flow_type = "UNKNOWN"

    main_depto = departamentos[0] if departamentos else "SIN_DEPTO"

    return {
        "has_bot": has_bot,
        "has_agent": has_agent,
        "first_bot_idx": first_bot_idx,
        "first_agent_idx": first_agent_idx,
        "flow_type": flow_type,
        "user_requested_agent": user_requested_agent,
        "bot_derived_to_agent": bot_derived_to_agent,
        "main_departamento": main_depto,
    }


# --- FORMATEO DE CONVERSACIONES -> TEXTO PARA PROMPT ---

def _format_conversation_block(conv: Dict[str, Any]) -> str:
    session_id = conv.get("session_id") or "SIN_SESSION_ID"
    telefono = conv.get("telefono") or conv.get("phone") or "SIN_TELEFONO"
    nombre = conv.get("nombre_cliente") or conv.get("nombre_completo") or "SIN_NOMBRE"
    mensajes_raw = conv.get("mensajes")
    mensajes_list = _normalize_mensajes(mensajes_raw)

    logger.info(
        "[QUALI] Conv %s: se normalizaron %d mensajes (raw_type=%s, raw_len=%s)",
        session_id,
        len(mensajes_list),
        type(mensajes_raw),
        len(mensajes_raw) if isinstance(mensajes_raw, str) else 0,
    )

    # Extraer meta para enriquecer el encabezado
    meta = _extract_conversation_meta(mensajes_list)
    flow_type = meta["flow_type"]
    main_depto = meta["main_departamento"]
    has_bot = "Sí" if meta["has_bot"] else "No"
    has_agent = "Sí" if meta["has_agent"] else "No"
    user_pide_agente = "Sí" if meta["user_requested_agent"] else "No"
    bot_deriva = "Sí" if meta["bot_derived_to_agent"] else "No"

    lineas: List[str] = []
    header = (
        f"Conversación {session_id} "
        f"(tel={telefono}, nombre={nombre}, depto={main_depto}, "
        f"flujo={flow_type}, tiene_bot={has_bot}, tiene_agente={has_agent}, "
        f"user_pide_agente={user_pide_agente}, bot_deriva_agente={bot_deriva})"
    )
    lineas.append(header)

    # Limitamos mensajes por conversación para no explotar tokens
    for m in mensajes_list[:MAX_MSGS_PER_CONV]:
        rol = (m.get("us_origen") or "bot").lower()
        texto = str(m.get("mensaje") or "").replace("\n", " ").strip()

        if not texto:
            continue

        if rol in ("user", "cliente", "client", "customer"):
            rol_fmt = "CLIENTE"
        elif rol in ("operator", "agent", "agente", "supervisor", "operator_bot"):
            rol_fmt = "AGENTE"
        elif rol in ("bot", "sistema", "system", "flow"):
            rol_fmt = "BOT"
        else:
            rol_fmt = rol.upper()

        lineas.append(f"[{rol_fmt}] {texto}")

    return "\n".join(lineas)


# --- PROMPT BUILDER ---

def build_qualitative_prompt(
    convs: List[Dict[str, Any]],
    lote_id: str,
    lote_num: int,
) -> str:
    convs_sampled = convs[:MAX_CONVS_IN_PROMPT]
    bloques: List[str] = []

    for conv in convs_sampled:
        bloques.append(_format_conversation_block(conv))

    cuerpo_convs = "\n\n---\n\n".join(bloques) if bloques else "SIN_CONVERSACIONES"

    prompt = (
        "Eres un analista experto en calidad de conversaciones de cobranza y atención al cliente.\n"
        "Analiza las siguientes conversaciones entre clientes, un BOT de cobranzas y agentes humanos.\n\n"
        "Cada bloque comienza con un encabezado que incluye meta-información útil:\n"
        "- depto: departamento lógico de la conversación (ej: Cobros Tardia, Customer Experience, etc.).\n"
        "- flujo: BOT_ONLY, AGENT_ONLY, BOT_TO_AGENT, OTHER.\n"
        "- tiene_bot / tiene_agente: si interviene o no cada actor.\n"
        "- user_pide_agente: si el cliente solicitó explícitamente hablar con un agente.\n"
        "- bot_deriva_agente: si el BOT informa que va a derivar con un agente.\n\n"
        f"Información del lote:\n"
        f"- lote_id: {lote_id}\n"
        f"- lote_num: {lote_num}\n"
        f"- cantidad de conversaciones en esta muestra: {len(convs_sampled)}\n\n"
        "Conversaciones (cada bloque pertenece a una sesión distinta):\n\n"
        f"{cuerpo_convs}\n\n"
        "Con base en estas conversaciones, genera un informe cualitativo ESTRUCTURADO en secciones numeradas.\n"
        "Sigue estas instrucciones:\n\n"
        "1) Resumen general del tipo de interacciones\n"
        "   - Describe el tono general (ej: amenazante, empático, neutro).\n"
        "   - Explica el contexto típico de las conversaciones (cobranza tardía, compromisos de pago, embargos, etc.).\n\n"
        "2) Quejas, demandas y posibles fraudes\n"
        "   - Identifica las principales quejas o demandas de los clientes.\n"
        "   - Señala cualquier indicio de quejas por fraude, malentendidos o percepción de abuso.\n"
        "   - Incluye 2-3 ejemplos concretos (fragmentos de mensajes breves entre comillas).\n\n"
        "3) Molestias o incomodidad con el BOT\n"
        "   - Detecta casos donde el cliente se muestra frustrado o confundido con el bot.\n"
        "   - Describe patrones: repetición de información, respuestas poco útiles, falta de empatía, etc.\n"
        "   - Incluye ejemplos puntuales.\n\n"
        "4) Evaluación de los agentes humanos\n"
        "   - Analiza si el agente dio una respuesta clara, ofreció alternativas y mostró empatía.\n"
        "   - Indica si, en los casos donde intervino un agente, el cliente quedó satisfecho o no (según el texto).\n"
        "   - Si aparecen varios agentes, agrupa por agente (por nombre o email si se menciona) y describe fortalezas y debilidades.\n"
        "   - Incluye ejemplos puntuales por agente.\n\n"
        "5) Colaboración BOT ↔ AGENTE\n"
        "   - Identifica cuándo el BOT necesita ayuda del agente (por ejemplo, deriva la conversación).\n"
        "   - Identifica si el agente parece apoyarse en información previa del BOT o repetir información ya dada.\n"
        "   - Resume la calidad del flujo BOT → AGENTE: ¿el traspaso es claro y oportuno?\n\n"
        "6) Solicitudes explícitas de hablar con un agente\n"
        "   - Señala cuántas conversaciones aproximadas incluyen pedidos explícitos de hablar con un agente.\n"
        "   - Describe cómo responde el sistema a esos pedidos (inmediato, tardío, poco claro, etc.).\n"
        "   - Incluye 1-2 ejemplos.\n\n"
        "7) Medición por actor (BOT y agentes)\n"
        "   - BOT: resume sus fortalezas (por ejemplo, claridad de templates, recordatorios, etc.) y sus debilidades.\n"
        "   - Agentes: para cada agente identificado, menciona brevemente su calidad de atención (explicación, empatía, presión, etc.).\n"
        '   - No hace falta dar métricas exactas, pero sí tendencias (ej: "la mayoría de las intervenciones de agentes son...").\n\n'
        "8) Flujo y tiempos de atención\n"
        '   - A partir de patrones como "¿Sigues ahí?", "aguardamos tu respuesta" y mensajes BOT_TO_AGENT, comenta:\n'
        "     • si hay tiempos de espera prolongados,\n"
        "     • si se nota abandono o falta de respuesta del cliente,\n"
        "     • si parece que se corta la conversación sin cierre claro.\n"
        "   - No es necesario dar segundos exactos; describe los patrones cualitativamente.\n\n"
        "9) Oportunidades concretas de mejora\n"
        "   - Propón mejoras específicas para:\n"
        "     • los guiones del BOT (texto, tono, orden),\n"
        "     • el entrenamiento de los agentes (frases a evitar, frases recomendadas),\n"
        "     • el diseño del flujo BOT → AGENTE.\n"
        '   - Sé concreto: "Cambiar X por Y" o "Agregar un paso Z".\n\n'
        "10) Ejemplos puntuales\n"
        "   - En cada sección donde tenga sentido, incluye 1-3 ejemplos textuales breves, entre comillas, sin repetir datos sensibles.\n\n"
        "Responde en español, en formato de informe con títulos, subtítulos y viñetas cuando sea útil.\n"
    )

    logger.info(
        "[QUALI] Prompt construido para lote_id=%s lote_num=%d: %d caracteres, %d conversaciones usadas",
        lote_id,
        lote_num,
        len(prompt),
        len(convs_sampled),
    )

    logger.info("[QUALI] Primeros 500 chars del prompt:\n%s", prompt[:500])

    return prompt


# --- GEMINI / VERTEX AI ---

def generate_gemini_summary(
    prompt: str,
    *,
    project_id: Optional[str] = None,
    location: Optional[str] = None,
    model_name: Optional[str] = None,
) -> str:
    if not _HAS_VERTEX:
        logger.warning(
            "vertexai no está instalado en este entorno; devolviendo mensaje de placeholder."
        )
        return (
            "ANÁLISIS CUALITATIVO (PLACEHOLDER)\n\n"
            "Aquí iría el resumen generado por Gemini. "
            "Instala y configura vertexai en el entorno para habilitar esta función."
        )

    project_id = (
        project_id
        or DEFAULT_VERTEX_PROJECT
        or os.environ.get("PROJECT_ID", "data-323821")
    )
    location = location or DEFAULT_VERTEX_LOCATION
    model_name = model_name or DEFAULT_VERTEX_MODEL

    logger.info(
        "[QUALI] Llamando a Gemini modelo=%s proyecto=%s ubicación=%s",
        model_name,
        project_id,
        location,
    )

    vertexai.init(project=project_id, location=location)
    model = GenerativeModel(model_name)

    resp = model.generate_content(
        prompt,
        generation_config=GenerationConfig(
            max_output_tokens=4096,
            temperature=0.3,
        ),
    )

    try:
        if hasattr(resp, "text") and resp.text:
            texto = resp.text
        else:
            textos: List[str] = []
            for cand in getattr(resp, "candidates", []) or []:
                content = getattr(cand, "content", None)
                if not content:
                    continue
                for part in getattr(content, "parts", []) or []:
                    t = getattr(part, "text", None)
                    if t:
                        textos.append(t)
            texto = "\n".join(textos)

        texto = (texto or "").strip()

        if not texto:
            logger.warning("[QUALI] Respuesta de Gemini sin texto utilizable.")
            return "No se pudo obtener texto utilizable desde la respuesta de Gemini."

        logger.info("[QUALI] Longitud del resumen generado: %d caracteres", len(texto))
        logger.info("[QUALI] Primeros 300 chars del resumen:\n%s", texto[:300])

        return texto

    except Exception as e:
        logger.error("[QUALI] Error procesando respuesta de Gemini: %s", e)
        return "Error procesando la respuesta de Gemini. Revisa los logs."


# --- ARCHIVO DE RESUMEN ---

def save_summary_file(
    summary_text: str,
    lote_id: str,
    lote_num: int,
    base_dir: str = "/tmp",
) -> str:
    os.makedirs(base_dir, exist_ok=True)
    safe_lote_id = lote_id.replace("-", "")
    filename = f"resumen_cualitativo_lote_{lote_num}_{safe_lote_id}.txt"
    path = os.path.join(base_dir, filename)

    logger.info(
        "[QUALI] Guardando resumen de %d caracteres en %s",
        len(summary_text or ""),
        path,
    )

    with open(path, "w", encoding="utf-8") as f:
        f.write(summary_text or "")

    logger.info("[QUALI] Resumen guardado en %s", path)
    return path


# --- ORQUESTADOR LOCAL DEL MÓDULO ---

def run_analisis_cualitativo(
    convs: List[Dict[str, Any]],
    *,
    lote_id: str,
    lote_num: int,
    project_id: Optional[str] = None,
    location: Optional[str] = None,
    model_name: Optional[str] = None,
    base_dir: str = "/tmp",
) -> Dict[str, Any]:
    if not convs:
        logger.warning(
            "[QUALI] No se proporcionaron conversaciones para análisis cualitativo."
        )
        empty_summary = "No se proporcionaron conversaciones para análisis cualitativo en este lote."
        return {
            "summary_text": empty_summary,
            "file_path": None,
            "n_convs_input": 0,
            "n_convs_used": 0,
        }

    logger.info(
        "[QUALI] run_analisis_cualitativo: lote_id=%s lote_num=%d n_convs_input=%d",
        lote_id,
        lote_num,
        len(convs),
    )

    prompt = build_qualitative_prompt(convs, lote_id=lote_id, lote_num=lote_num)

    summary_text = generate_gemini_summary(
        prompt,
        project_id=project_id,
        location=location,
        model_name=model_name,
    )

    file_path = save_summary_file(
        summary_text, lote_id=lote_id, lote_num=lote_num, base_dir=base_dir
    )

    return {
        "summary_text": summary_text,
        "file_path": file_path,
        "n_convs_input": len(convs),
        "n_convs_used": min(len(convs), MAX_CONVS_IN_PROMPT),
    }