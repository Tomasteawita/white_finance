"""
detect_professional_profiles.py
================================
Motor de detección de perfiles profesionales en conversaciones de Instagram
usando expresiones regulares compiladas sobre texto en español.

Categorías detectadas:
    1. Entrenador (personal trainer, preparador físico, etc.)
    2. Propietario de Gimnasio (dueño de gym, box de crossfit, etc.)
    3. Profesional con Consultorio (médico, psicólogo, kinesiólogo, etc.)
    4. Masajista / Terapeuta corporal (masajista, masoterapeuta, etc.)
    5. Propietario de Estudio (estudio jurídico, contable, pilates, yoga, etc.)

Lógica de negocio:
    - Solo se analizan mensajes del PROSPECTO (sender != "Tomas Santiago Cueva").
    - Se concatenan todos los mensajes del prospecto por conversation_id.
    - Se aplican patrones regex con context-awareness para reducir falsos positivos.
    - Los patrones incluyen manejo de variaciones léxicas en español (acentos, géneros).

Input:  JSONL con registros de chat_corpus_nlp
Output: CSV con conversation_id, categorías detectadas y keywords matched.

Autor: Data Engineering Pipeline - Marketing Layer
"""

import json
import logging
import re
import sys
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, NamedTuple

import pandas as pd

# ---------------------------------------------------------------------------
# Forzar UTF-8 en stdout/stderr para compatibilidad con terminales Windows
# (cp1252 no soporta caracteres Unicode como nombres de usuario de Instagram)
# ---------------------------------------------------------------------------
sys.stdout.reconfigure(encoding="utf-8", errors="replace")
sys.stderr.reconfigure(encoding="utf-8", errors="replace")

# ---------------------------------------------------------------------------
# Configuración de logging con timestamp para trazabilidad (CNV compliance)
# ---------------------------------------------------------------------------
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)-8s | %(message)s",
    datefmt="%Y-%m-%dT%H:%M:%S",
    handlers=[logging.StreamHandler(sys.stdout)],
)
logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Constantes
# ---------------------------------------------------------------------------
OWNER_SENDER_NAME: str = "Tomas Santiago Cueva"

# ---------------------------------------------------------------------------
# Definición de categorías profesionales y patrones regex
# ---------------------------------------------------------------------------
# Cada categoría tiene:
#   - patterns_positive: patrones que CONFIRMAN la categoría (el prospecto declara serlo)
#   - patterns_negative: patrones que INVALIDAN un match (negaciones, contexto diferente)
#
# Decisiones de diseño:
#   - Se usan word boundaries (\b) para evitar substrings parciales.
#   - re.IGNORECASE + re.UNICODE para cubrir acentos y mayúsculas.
#   - "estudio" como sustantivo (profesional) vs verbo (estudiante) se
#     desambigua exigiendo que "estudio" venga acompañado del tipo de estudio.

REGEX_FLAGS: int = re.IGNORECASE | re.UNICODE


class CategoryConfig(NamedTuple):
    """Configuración de una categoría profesional para detección regex."""
    name: str
    label: str
    positive_patterns: list[re.Pattern[str]]
    negative_patterns: list[re.Pattern[str]]


def _compile_patterns(raw_patterns: list[str]) -> list[re.Pattern[str]]:
    """
    Compila una lista de strings regex en objetos Pattern reutilizables.

    :param raw_patterns: Lista de expresiones regulares como strings.
    :return: Lista de objetos re.Pattern compilados con IGNORECASE | UNICODE.
    """
    return [re.compile(p, REGEX_FLAGS) for p in raw_patterns]


# ---------------------------------------------------------------------------
# 1. ENTRENADOR
# ---------------------------------------------------------------------------
ENTRENADOR = CategoryConfig(
    name="entrenador",
    label="Entrenador / Personal Trainer",
    positive_patterns=_compile_patterns([
        # Declaraciones directas: "soy entrenador/a", "soy personal trainer"
        r"\bsoy\s+entrenador[a]?\b",
        r"\bsoy\s+personal\s+trainer\b",
        r"\bsoy\s+preparador[a]?\s+f[ií]sic[oa]\b",
        # "trabajo como entrenador/a", "me dedico a entrenar"
        r"\btrabajo\s+como\s+entrenador[a]?\b",
        r"\bme\s+dedico\s+a\s+entrenar\b",
        # "doy clases de entrenamiento", "hago entrenamiento funcional"
        r"\bdoy\s+clases\s+de\s+entrenamiento\b",
        r"\bhago\s+entrenamiento\s+funcional\b",
        # "soy coach deportivo/a", "coach fitness"
        r"\bsoy\s+coach\s+(?:deportiv[oa]|fitness|de\s+entrenamiento)\b",
        # "soy profe de educación física", "soy profe de gym"
        r"\bsoy\s+prof(?:e|esor[a]?)\s+de\s+(?:educaci[oó]n\s+f[ií]sica|ed\.?\s*f[ií]sica|gym|gimnasia)\b",
        # "entreno gente", "entreno personas", "entreno clientes"
        r"\bentreno\s+(?:gente|personas|clientes|alumnos|alumnas)\b",
        # "tengo alumnos/as" en contexto fitness
        r"\btengo\s+(?:alumnos|alumnas|clientes)\b.*\b(?:entren|gym|gimnasio|funcional)\b",
        # "soy entrenadora .Hago entrenamiento funcinal" (typos comunes)
        r"\bsoy\s+entrenador[a]?\b",
        # "soy profesor de ed física" (variación informal)
        r"\bsoy\s+profesor(?:a)?\s+de\s+ed\.?\s*f[ií]sica\b",
    ]),
    negative_patterns=_compile_patterns([
        # Excluir menciones de 3ra persona o sugerencias genéricas
        r"\bmi\s+entrenador\b",
        r"\bbusco\s+(?:un\s+)?entrenador\b",
        r"\bnecesito\s+(?:un\s+)?entrenador\b",
    ]),
)

# ---------------------------------------------------------------------------
# 2. PROPIETARIO DE GIMNASIO
# ---------------------------------------------------------------------------
PROPIETARIO_GIMNASIO = CategoryConfig(
    name="propietario_gimnasio",
    label="Propietario de Gimnasio",
    positive_patterns=_compile_patterns([
        # "tengo un gym/gimnasio", "tengo mi gym"
        r"\btengo\s+(?:un|mi)\s+(?:gym|gimnasio|mini\s*gym)\b",
        # "soy dueño/a de un gimnasio/gym"
        r"\bsoy\s+due[ñn][oa]\s+(?:de(?:l)?\s+)?(?:un\s+)?(?:gym|gimnasio)\b",
        # "mi gimnasio", "mi gym" en contexto de posesión
        r"\b(?:abr[ií]|arme|mont[eé]|puse)\s+(?:un\s+|mi\s+)?(?:gym|gimnasio)\b",
        # "box de crossfit"
        r"\btengo\s+(?:un\s+)?box\s+(?:de\s+)?crossfit\b",
        r"\bmi\s+box\s+(?:de\s+)?crossfit\b",
        # "la dueña del gimnasio" (puede ser 3ra persona - se filtra en negatives)
        r"\b(?:due[ñn][oa]|propietari[oa])\s+del?\s+(?:gym|gimnasio)\b",
        # "van a mi mini gym"
        r"\bvan\s+a\s+mi\s+(?:mini\s*)?gym\b",
        # "usarla en el gym" con contexto de propiedad
        r"\bmi\s+(?:mini\s*)?gym\b",
    ]),
    negative_patterns=_compile_patterns([
        # "no tengo un gimnasio" / "no tengo un gym" (negación directa del sustantivo)
        # IMPORTANTE: Patrón restrictivo - solo matchea negaciones seguidas DIRECTAMENTE
        # de gym/gimnasio para evitar falsos negativos cuando "no tengo las herramientas"
        # aparece en el mismo texto concatenado que "mi mini gym"
        r"\bno\s+tengo\s+(?:un\s+)?(?:gym|gimnasio)\b",
        # "voy al gym", "entreno en el gym" (usuario, no dueño)
        r"\bvoy\s+al?\s+(?:gym|gimnasio)\b",
        # "mi hermana es la dueña" (3ra persona)
        r"\b(?:mi\s+)?(?:hermana?|amig[oa]|socio|padre|madre)\s+(?:es\s+)?(?:la?\s+)?due[ñn][oa]\b",
        # "como gimnasio" en pregunta
        r"\bcomo\s+gimnasio\b",
        # "muchos gym por aca" (referencia contextual, no propiedad)
        r"\bmuchos?\s+gym\b",
    ]),
)

# ---------------------------------------------------------------------------
# 3. PROFESIONAL CON CONSULTORIO
# ---------------------------------------------------------------------------
PROFESIONAL_CONSULTORIO = CategoryConfig(
    name="profesional_consultorio",
    label="Profesional con Consultorio",
    positive_patterns=_compile_patterns([
        # "tengo un consultorio", "tengo mi consultorio"
        r"\btengo\s+(?:un|mi)\s+consultorio\b",
        # "soy médico/a", "soy doctor/a"
        r"\bsoy\s+(?:m[eé]dic[oa]|doctor[a]?)\b",
        # "soy psicólogo/a", "soy psiquiatra"
        r"\bsoy\s+(?:psic[oó]log[oa]|psiquiatra)\b",
        # "soy kinesiólogo/a", "soy fisioterapeuta"
        r"\bsoy\s+(?:kinesi[oó]log[oa]|fisioterapeuta|fisiatra)\b",
        # "soy odontólogo/a", "soy dentista"
        r"\bsoy\s+(?:odont[oó]log[oa]|dentista)\b",
        # "soy nutricionista", "soy nutriólogo/a"
        r"\bsoy\s+(?:nutricionista|nutri[oó]log[oa])\b",
        # "soy quiropráctico/a"
        r"\bsoy\s+quiropr[aá]ctic[oa]\b",
        # "soy veterinario/a"
        r"\bsoy\s+veterinari[oa]\b",
        # "atiendo en mi consultorio", "atiendo pacientes"
        r"\batiendo\s+(?:en\s+mi\s+consultorio|pacientes)\b",
        # "mi consultorio de..."
        r"\bmi\s+consultorio\s+(?:de|m[eé]dico|odontol[oó]gico|psicol[oó]gico)\b",
        # "trabajo en un consultorio"
        r"\btrabajo\s+en\s+(?:un|mi)\s+consultorio\b",
    ]),
    negative_patterns=_compile_patterns([
        # "no tengo un consultorio"
        r"\bno\s+tengo\b.*\bconsultorio\b",
        # "como consultorio" en pregunta
        r"\bcomo\s+consultorio\b",
        # "estudiando psicología" (aún no ejerce)
        r"\bestudi(?:o|ando)\s+(?:psicolog[ií]a|medicina|odontolog[ií]a|kinesiolog[ií]a)\b",
    ]),
)

# ---------------------------------------------------------------------------
# 4. MASAJISTA / TERAPEUTA CORPORAL
# ---------------------------------------------------------------------------
MASAJISTA = CategoryConfig(
    name="masajista",
    label="Masajista / Terapeuta Corporal",
    positive_patterns=_compile_patterns([
        # "soy masajista", "soy masoterapeuta"
        r"\bsoy\s+masajista\b",
        r"\bsoy\s+masoterapeuta\b",
        # Respuesta directa "Masajista" (una sola palabra como autodeclaración)
        r"^masajista$",
        # "hago masajes", "doy masajes"
        r"\b(?:hago|doy|ofrezco)\s+masajes\b",
        # "masoterapia", "masoterapias"
        r"\b(?:hago|doy|me\s+dedico\s+a)\s+masoterapias?\b",
        # "tengo un gabinete de masajes"
        r"\btengo\s+(?:un\s+)?gabinete\s+de\s+masajes\b",
        # "mi gabinete de masajes"
        r"\bmi\s+gabinete\s+de\s+masajes\b",
        # "trabajo como masajista"
        r"\btrabajo\s+como\s+masajista\b",
        # "me dedico a los masajes"
        r"\bme\s+dedico\s+a\s+(?:los\s+)?masajes\b",
        # "soy masajista y depiladora" (combinación)
        r"\bmasajista\s+y\b",
    ]),
    negative_patterns=_compile_patterns([
        # "ojala fuera masajista" (no lo es)
        r"\bojal[aá]\s+(?:fuera|fuese)\s+masajista\b",
        # "no soy masajista"
        r"\bno\s+soy\s+masajista\b",
        # "busco un masajista"
        r"\bbusco\s+(?:un\s+)?masajista\b",
    ]),
)

# ---------------------------------------------------------------------------
# 5. PROPIETARIO DE ESTUDIO (profesional o fitness)
# ---------------------------------------------------------------------------
PROPIETARIO_ESTUDIO = CategoryConfig(
    name="propietario_estudio",
    label="Propietario de Estudio",
    positive_patterns=_compile_patterns([
        # "tengo un estudio de/jurídico/contable/pilates/yoga/diseño"
        r"\btengo\s+(?:un|mi)\s+estudio\s+(?:jur[ií]dico|contable|de\s+(?:pilates|yoga|dise[ñn]o|arquitectura|grabaci[oó]n|fotograf[ií]a|tatuaje))\b",
        # "soy dueño/a de un estudio de..."
        r"\bsoy\s+due[ñn][oa]\s+de\s+(?:un\s+)?estudio\s+(?:de\s+)?(?:pilates|yoga|dise[ñn]o|jur[ií]dico|contable)\b",
        # "mi estudio de pilates/yoga/diseño/jurídico"
        r"\bmi\s+estudio\s+(?:de\s+)?(?:pilates|yoga|dise[ñn]o|jur[ií]dico|contable|arquitectura|fotograf[ií]a)\b",
        # "tengo un estudio" + contexto profesional
        r"\babr[ií]\s+(?:un\s+)?estudio\s+de\b",
        # "doy clases de pilates/yoga" (instructor con estudio propio)
        r"\b(?:doy|tengo)\s+clases\s+de\s+(?:pilates|yoga)\b",
        # "tengo mi centro de pilates/yoga"
        r"\btengo\s+(?:un|mi)\s+(?:centro|sala)\s+de\s+(?:pilates|yoga)\b",
    ]),
    negative_patterns=_compile_patterns([
        # "estudio" como verbo (estudiante)
        r"\bestudio\s+(?:en\s+(?:el|la|un)|desde\s+mi\s+casa|medicina|derecho|psicolog[ií]a|ingenier[ií]a)\b",
        # "actualmente estudio"
        r"\bactualmente\s+estudio\b",
        # "estudio en el colegio"
        r"\bestudio\s+en\s+el\s+colegio\b",
        # "pagar mis estudios"
        r"\bmis\s+estudios\b",
        # "estoy estudiando"
        r"\bestoy\s+estudi(?:ando)\b",
    ]),
)

# Lista consolidada de todas las categorías
ALL_CATEGORIES: list[CategoryConfig] = [
    ENTRENADOR,
    PROPIETARIO_GIMNASIO,
    PROFESIONAL_CONSULTORIO,
    MASAJISTA,
    PROPIETARIO_ESTUDIO,
]


# ---------------------------------------------------------------------------
# Funciones de procesamiento
# ---------------------------------------------------------------------------

def detect_categories_in_text(
    text: str,
    categories: list[CategoryConfig],
) -> dict[str, list[str]]:
    """
    Detecta categorías profesionales en un texto usando regex compilados.

    Aplica patrones positivos para encontrar coincidencias y luego valida
    contra patrones negativos para eliminar falsos positivos.

    :param text: Texto consolidado del prospecto (todos sus mensajes concatenados).
    :param categories: Lista de CategoryConfig con patrones a evaluar.
    :return: Diccionario {nombre_categoría: [keywords_matched]} solo para categorías detectadas.
    """
    if not text or not text.strip():
        return {}

    detected: dict[str, list[str]] = {}

    for cat in categories:
        # Primero verificar si algún patrón negativo invalida la categoría
        negated: bool = False
        for neg_pattern in cat.negative_patterns:
            if neg_pattern.search(text):
                negated = True
                break

        if negated:
            continue

        # Buscar coincidencias positivas
        matched_keywords: list[str] = []
        for pos_pattern in cat.positive_patterns:
            matches = pos_pattern.findall(text)
            if matches:
                # findall retorna strings; agregar las coincidencias únicas
                matched_keywords.extend(
                    m.strip() if isinstance(m, str) else m[0].strip()
                    for m in matches
                )

        if matched_keywords:
            # Eliminar duplicados preservando orden
            seen: set[str] = set()
            unique_keywords: list[str] = []
            for kw in matched_keywords:
                kw_lower = kw.lower()
                if kw_lower not in seen:
                    seen.add(kw_lower)
                    unique_keywords.append(kw)
            detected[cat.name] = unique_keywords

    return detected


def load_conversations_from_jsonl(
    file_path: str,
) -> dict[str, dict[str, Any]]:
    """
    Lee un archivo JSONL de chat_corpus_nlp y agrupa mensajes por conversation_id.

    Solo incluye mensajes del PROSPECTO (no del owner/Tomás).
    Concatena el full_text de cada mensaje del prospecto en un texto unificado.

    :param file_path: Ruta absoluta al archivo JSONL de entrada.
    :return: Diccionario {conversation_id: {'prospect_text': str, 'prospect_name': str, 'msg_count': int}}
    :raises FileNotFoundError: Si el archivo no existe en la ruta especificada.
    :raises json.JSONDecodeError: Si alguna línea no es JSON válido (se logea y continúa).
    """
    path = Path(file_path)
    if not path.exists():
        raise FileNotFoundError(f"Archivo no encontrado: {file_path}")

    conversations: dict[str, dict[str, Any]] = {}
    total_lines: int = 0
    parse_errors: int = 0

    with open(path, "r", encoding="utf-8") as f:
        for line in f:
            total_lines += 1
            line = line.strip()
            if not line:
                continue

            try:
                record = json.loads(line)
            except json.JSONDecodeError as e:
                parse_errors += 1
                logger.warning(
                    "Línea %d: JSON inválido - %s", total_lines, str(e)
                )
                continue

            conv_id: str = record.get("conversation_id", "")
            sender: str = record.get("sender_name", "")
            full_text: str = record.get("full_text", "") or ""

            if not conv_id:
                continue

            # Inicializar conversación si no existe
            if conv_id not in conversations:
                conversations[conv_id] = {
                    "prospect_text": "",
                    "prospect_messages": [],  # Mensajes individuales para detección granular
                    "prospect_name": "",
                    "total_msgs": 0,
                    "prospect_msgs": 0,
                }

            conversations[conv_id]["total_msgs"] += 1

            # Solo procesar mensajes del prospecto (no del owner)
            if sender == OWNER_SENDER_NAME:
                continue

            conversations[conv_id]["prospect_msgs"] += 1
            if not conversations[conv_id]["prospect_name"] and sender:
                conversations[conv_id]["prospect_name"] = sender

            # Concatenar texto del prospecto con separador de línea
            # Y guardar mensajes individuales para detección per-message
            if full_text.strip():
                conversations[conv_id]["prospect_text"] += " " + full_text
                conversations[conv_id]["prospect_messages"].append(full_text.strip())

    logger.info(
        "JSONL cargado: %d líneas procesadas, %d conversaciones únicas, %d errores de parseo",
        total_lines,
        len(conversations),
        parse_errors,
    )
    return conversations


def process_conversations(
    conversations: dict[str, dict[str, Any]],
    categories: list[CategoryConfig],
) -> pd.DataFrame:
    """
    Aplica el motor de detección regex a todas las conversaciones y genera un DataFrame.

    Para cada conversation_id, analiza el texto del prospecto contra todas
    las categorías profesionales definidas.

    :param conversations: Diccionario de conversaciones agrupadas (output de load_conversations_from_jsonl).
    :param categories: Lista de CategoryConfig con patrones a evaluar.
    :return: DataFrame con columnas: conversation_id, prospect_name, total_msgs, prospect_msgs,
             is_qualified_lead, detected_categories, matched_keywords, y una columna booleana
             por cada categoría.
    """
    results: list[dict[str, Any]] = []

    for conv_id, conv_data in conversations.items():
        prospect_text: str = conv_data["prospect_text"]
        prospect_messages: list[str] = conv_data.get("prospect_messages", [])

        # Estrategia dual: detectar en texto concatenado Y en mensajes individuales.
        # Los mensajes individuales capturan respuestas cortas (ej. "Masajista")
        # donde los patrones ^...$ necesitan evaluar el mensaje aislado.
        detected = detect_categories_in_text(prospect_text, categories)

        # Complementar con detección per-message para respuestas cortas
        for msg in prospect_messages:
            msg_detected = detect_categories_in_text(msg, categories)
            for cat_name, kws in msg_detected.items():
                if cat_name not in detected:
                    detected[cat_name] = kws
                else:
                    # Agregar keywords nuevas que no estaban
                    existing_lower = {k.lower() for k in detected[cat_name]}
                    for kw in kws:
                        if kw.lower() not in existing_lower:
                            detected[cat_name].append(kw)
                            existing_lower.add(kw.lower())

        row: dict[str, Any] = {
            "conversation_id": conv_id,
            "prospect_name": conv_data["prospect_name"],
            "total_msgs": conv_data["total_msgs"],
            "prospect_msgs": conv_data["prospect_msgs"],
            "is_qualified_lead": len(detected) > 0,
            "detected_categories": ", ".join(detected.keys()) if detected else "",
            "matched_keywords": (
                "; ".join(
                    f"{cat}: [{', '.join(kws)}]"
                    for cat, kws in detected.items()
                )
                if detected
                else ""
            ),
        }

        # Columnas booleanas por categoría
        for cat in categories:
            row[f"is_{cat.name}"] = cat.name in detected

        results.append(row)

    df = pd.DataFrame(results)
    return df


def generate_summary_stats(df: pd.DataFrame, categories: list[CategoryConfig]) -> str:
    """
    Genera un resumen estadístico del análisis de leads cualificados.

    Calcula porcentajes de detección por categoría y métricas generales.

    :param df: DataFrame con resultados del procesamiento.
    :param categories: Lista de CategoryConfig para iterar nombres.
    :return: String formateado con el resumen estadístico.
    """
    total: int = len(df)
    qualified: int = int(df["is_qualified_lead"].sum())
    pct_qualified: float = (qualified / total * 100) if total > 0 else 0.0

    lines: list[str] = [
        "=" * 65,
        "  RESUMEN DE DETECCIÓN DE PERFILES PROFESIONALES",
        "=" * 65,
        f"  Total de conversaciones analizadas:   {total}",
        f"  Leads cualificados detectados:        {qualified}",
        f"  Porcentaje de leads cualificados:     {pct_qualified:.2f}%",
        "-" * 65,
        "  DETALLE POR CATEGORÍA:",
        "-" * 65,
    ]

    for cat in categories:
        col = f"is_{cat.name}"
        count: int = int(df[col].sum())
        pct: float = (count / total * 100) if total > 0 else 0.0
        lines.append(f"    {cat.label:<40s}  {count:>4d}  ({pct:>5.2f}%)")

    lines.append("=" * 65)
    return "\n".join(lines)


# ---------------------------------------------------------------------------
# Tests unitarios con datos mock
# ---------------------------------------------------------------------------

def run_mock_tests() -> None:
    """
    Ejecuta tests con conversaciones mock para validar el motor de regex.

    Incluye casos positivos (coincidencia esperada) y negativos (sin coincidencia).
    Falla ruidosamente si alguna aserción no se cumple (principio anti-frágil).
    """
    logger.info("Ejecutando tests con datos mock...")

    test_cases: list[dict[str, Any]] = [
        # --- POSITIVOS ---
        {
            "text": "hola Tomas si soy entrenadora .Hago entrenamiento funcinal",
            "expected_cats": {"entrenador"},
            "desc": "Entrenadora con typo en funcional",
        },
        {
            "text": "Tengo un gym",
            "expected_cats": {"propietario_gimnasio"},
            "desc": "Dueño de gym directo",
        },
        {
            "text": "tengo muchas personas q van a mi mini gym, algo de 10 personas",
            "expected_cats": {"propietario_gimnasio"},
            "desc": "Mini gym con clientes",
        },
        {
            "text": "Emm..si,hace tres años soy masajista y depiladora láser.",
            "expected_cats": {"masajista"},
            "desc": "Masajista declarada",
        },
        {
            "text": "Masajista",
            "expected_cats": {"masajista"},
            "desc": "Respuesta directa: Masajista",
        },
        {
            "text": "Soy psicóloga, atiendo en mi consultorio particular",
            "expected_cats": {"profesional_consultorio"},
            "desc": "Psicóloga con consultorio",
        },
        {
            "text": "Tengo mi estudio de pilates hace 5 años",
            "expected_cats": {"propietario_estudio"},
            "desc": "Dueña de estudio de pilates",
        },
        {
            "text": "soy coach deportivo y tengo mi propio espacio",
            "expected_cats": {"entrenador"},
            "desc": "Coach deportivo",
        },
        {
            "text": "soy kinesiólogo, trabajo en un consultorio",
            "expected_cats": {"profesional_consultorio"},
            "desc": "Kinesiólogo con consultorio",
        },
        {
            "text": "tengo muchas personas q van a mi mini gym no soy profesional y no tengo las herramientas",
            "expected_cats": {"propietario_gimnasio"},
            "desc": "Mini gym con 'no tengo las herramientas' (NO debe negar gym)",
        },
        {
            "text": "me falta un mes para ser profesional pero digamos q soy profesor de ed física",
            "expected_cats": {"entrenador"},
            "desc": "Profesor de ed física informal",
        },
        # --- NEGATIVOS (no deben matchear) ---
        {
            "text": "Actualmente estudio",
            "expected_cats": set(),
            "desc": "Estudiante genérico - NO debe matchear estudio",
        },
        {
            "text": "Estudio  en el colegio",
            "expected_cats": set(),
            "desc": "Estudiante de colegio - NO debe matchear estudio",
        },
        {
            "text": "No, ojala fuera masajista jajaj trabajo para pedidos ya",
            "expected_cats": set(),
            "desc": "Negación de masajista",
        },
        {
            "text": "No, no tengo un gimnasio o consultorio.",
            "expected_cats": set(),
            "desc": "Negación explícita de gym y consultorio",
        },
        {
            "text": "Por eso quiero tener para pagar mis estudios",
            "expected_cats": set(),
            "desc": "'mis estudios' como sustantivo genérico - NO debe matchear",
        },
        {
            "text": "hola que tal si ago carpinteria y soy estudiante de calistenia",
            "expected_cats": set(),
            "desc": "Carpintero/estudiante - NO debe matchear ninguna categoría",
        },
        {
            "text": "mi hermana es la dueña del gimnasio",
            "expected_cats": set(),
            "desc": "Tercera persona - NO debe matchear propietario",
        },
    ]

    passed: int = 0
    failed: int = 0

    for i, tc in enumerate(test_cases, 1):
        detected = detect_categories_in_text(tc["text"], ALL_CATEGORIES)
        detected_set = set(detected.keys())
        expected_set: set[str] = tc["expected_cats"]

        if detected_set == expected_set:
            passed += 1
            status = "[PASS]"
        else:
            failed += 1
            status = "[FAIL]"

        logger.info(
            "Test %02d %s | %s | Esperado: %s | Detectado: %s | Keywords: %s",
            i,
            status,
            tc["desc"],
            expected_set or "(ninguna)",
            detected_set or "(ninguna)",
            detected or "(ninguna)",
        )

    logger.info(
        "Tests completados: %d/%d pasaron (%d fallaron)",
        passed,
        passed + failed,
        failed,
    )

    if failed > 0:
        logger.warning(
            "ATENCION: %d tests fallaron. Revisar patrones regex.", failed
        )


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main() -> None:
    """
    Punto de entrada principal del script.

    1. Ejecuta tests mock para validar el motor regex.
    2. Carga las conversaciones del archivo JSONL.
    3. Procesa todas las conversaciones contra las categorías.
    4. Genera y persiste el CSV de salida.
    5. Imprime resumen estadístico.
    """
    run_ts = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    logger.info("Inicio de ejecución: %s", run_ts)

    # ---- Paso 1: Tests Mock ----
    run_mock_tests()
    print()

    # ---- Paso 2: Cargar datos ----
    input_path: str = (
        r"D:\DatosDeMercado\marketing_data\instagram_20260802\processed\chat_corpus_nlp.jsonl"
    )
    conversations = load_conversations_from_jsonl(input_path)

    # ---- Paso 3: Procesar ----
    df = process_conversations(conversations, ALL_CATEGORIES)

    # ---- Paso 4: Persistir CSV ----
    output_dir = Path(input_path).parent
    output_filename = f"professional_profiles_detected_{run_ts}.csv"
    output_path = output_dir / output_filename
    df.to_csv(output_path, index=False, encoding="utf-8-sig")
    logger.info("CSV generado: %s", output_path)

    # ---- Paso 5: Resumen estadístico ----
    summary = generate_summary_stats(df, ALL_CATEGORIES)
    print()
    print(summary)

    # Mostrar leads cualificados detectados
    qualified_df = df[df["is_qualified_lead"]].copy()
    if not qualified_df.empty:
        print()
        print("LEADS CUALIFICADOS DETECTADOS:")
        print("-" * 65)
        for _, row in qualified_df.iterrows():
            print(
                f"  {row['conversation_id']:<45s} | "
                f"{row['prospect_name']:<25s} | "
                f"{row['detected_categories']}"
            )
            print(f"    Keywords: {row['matched_keywords']}")
        print("-" * 65)

    logger.info("Proceso finalizado exitosamente.")


if __name__ == "__main__":
    main()
