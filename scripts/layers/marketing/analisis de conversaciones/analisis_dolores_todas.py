"""
analisis_dolores_todas.py
=========================
Extrae y consolida los "dolores" (pain points) de TODAS las conversaciones 
registradas, independientemente de si iniciaron con FLUJO o no.

Pipeline:
1. Carga el CSV de transcripciones de la nueva fecha (2026-08-20).
2. Identifica conversaciones con interacciones del cliente.
3. Reconstruye el diálogo completo de cada conversación.
4. Envía las conversaciones a Gemini Flash en batches para extraer el dolor principal.
5. Consolida y agrupa dolores similares semánticamente.
6. Genera CSV final.
"""

import os
import json
import time
import logging
import re
from pathlib import Path

import pandas as pd
from google import genai
from google.genai import types
from dotenv import load_dotenv

# ─────────────────────────────────────────────
# CONFIGURACION
# ─────────────────────────────────────────────
load_dotenv()

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S"
)
log = logging.getLogger(__name__)

INPUT_CSV = Path("D:/DatosDeMercado/marketing_data/instagram_20260820/processed/instagram_audio_transcripts.csv")
OUTPUT_DIR = Path("D:/DatosDeMercado/marketing_data/instagram_20260820/processed")
OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

OUTPUT_DOLORES = OUTPUT_DIR / "dolores_todas_consolidados.csv"
OUTPUT_POR_CONV = OUTPUT_DIR / "dolores_todas_por_conversacion.csv"

GEMINI_API_KEY = os.getenv("GEMINI_API_KEY", "")
GEMINI_MODEL = "gemini-flash-latest"
BATCH_SIZE = 20                      
SLEEP_BETWEEN_BATCHES = 3            
MAX_RETRIES = 3                      

TOMAS_NAME_PATTERN = "Cueva"


# ─────────────────────────────────────────────
# PASO 1: CARGA Y FILTRADO
# ─────────────────────────────────────────────

def cargar_y_filtrar_todas(csv_path: Path) -> tuple[pd.DataFrame, list[str]]:
    """
    Carga el CSV y retorna el dataframe completo y los IDs de conversaciones.
    Filtra solo para asegurarse de que el cliente haya interactuado.
    """
    log.info(f"Cargando CSV: {csv_path}")
    df = pd.read_csv(csv_path, encoding="latin-1", on_bad_lines="skip")
    log.info(f"Total filas: {len(df):,} | Conversaciones únicas: {df['conversation_id'].nunique():,}")

    df_cliente = df[~df["sender_name"].str.contains(TOMAS_NAME_PATTERN, na=False)].copy()
    
    todas_ids = df_cliente["conversation_id"].unique().tolist()
    log.info(f"Conversaciones con al menos un mensaje del cliente: {len(todas_ids)}")

    df_todas = df[df["conversation_id"].isin(todas_ids)].copy()
    return df_todas, todas_ids


# ─────────────────────────────────────────────
# PASO 2: RECONSTRUCCION DE DIALOGOS
# ─────────────────────────────────────────────

def reconstruir_dialogo(df_conv: pd.DataFrame, conversation_id: str) -> str:
    """Reconstruye el dialogo completo de una conversacion como texto plano."""
    conv = df_conv[df_conv["conversation_id"] == conversation_id].sort_values("timestamp_ms")
    
    IGNORAR_PATRONES = [
        "you sent an attachment", "sent an attachment",
        "reacted", "replied to an ad", "reminder:", "view ad",
        "https://", "http://"
    ]
    
    lines = []
    for _, row in conv.iterrows():
        texto = str(row.get("full_text", "")).strip()
        if not texto or texto.lower() == "nan":
            continue
        texto_lower = texto.lower()
        if any(p in texto_lower for p in IGNORAR_PATRONES):
            continue
        
        es_tomas = TOMAS_NAME_PATTERN in str(row["sender_name"])
        rol = "TOMAS" if es_tomas else "CLIENTE"
        if len(texto) > 500:
            texto = texto[:500] + "..."
        lines.append(f"[{rol}]: {texto}")
    
    return "\n".join(lines) if lines else "[CONVERSACION SIN CONTENIDO TEXTUAL]"


# ─────────────────────────────────────────────
# PASO 3: EXTRACCION DE DOLORES VIA GEMINI
# ─────────────────────────────────────────────

def construir_prompt_batch(conversaciones: list) -> str:
    """Construye el prompt para un batch de conversaciones."""
    conv_texts = []
    for conv_id, dialogo in conversaciones:
        conv_texts.append(
            f"--- CONVERSACION ID: {conv_id} ---\n{dialogo}\n--- FIN CONVERSACION ---"
        )
    batch_text = "\n\n".join(conv_texts)
    
    prompt = (
        "Eres un experto en analisis de marketing y ventas para profesionales independientes latinoamericanos.\n"
        "Analiza las siguientes conversaciones de Instagram DMs entre un asesor financiero (TOMAS) "
        "y clientes potenciales (CLIENTE).\n\n"
        "Para cada conversacion, identifica el DOLOR PRINCIPAL que expresa el cliente.\n"
        "El dolor es el problema, frustracion o necesidad mas importante que menciona el cliente.\n\n"
        "REGLAS:\n"
        "1. Si el cliente no expresa ningun dolor concreto (por ejemplo solo saluda, reacciona o pide precio sin contexto), usa: Sin dolor expresado\n"
        "2. Se especifico pero generalizable: no uses nombres propios.\n"
        "   Usa categorias como: no llego a fin de mes, no se como ahorrar, gastos mezclados negocio y personal, etc.\n"
        "3. Usa espanol rioplatense/argentino natural.\n"
        "4. Un solo dolor por conversacion (el mas importante).\n"
        "5. Maximo 10 palabras por dolor.\n\n"
        "Responde UNICAMENTE con JSON valido en este formato:\n"
        '{"resultados": [{"conversation_id": "ID_AQUI", "dolor": "descripcion del dolor"}, ...]}\n\n'
        "CONVERSACIONES:\n\n"
        f"{batch_text}"
    )
    return prompt


def extraer_dolores_con_gemini(
    todas_ids: list,
    df_todas: pd.DataFrame,
    api_key: str
) -> pd.DataFrame:
    """
    Envia conversaciones a Gemini en batches y extrae el dolor por conversacion.
    Usa la nueva SDK google-genai.
    """
    if not api_key:
        raise ValueError("GEMINI_API_KEY no configurada.")
    
    client = genai.Client(api_key=api_key)
    
    log.info(f"Iniciando extraccion con modelo: {GEMINI_MODEL}")
    log.info(f"Total conversaciones: {len(todas_ids)} | Batch size: {BATCH_SIZE}")
    
    resultados_todos = []
    batches_fallidos = []
    
    dialogos = [
        (conv_id, reconstruir_dialogo(df_todas, conv_id))
        for conv_id in todas_ids
    ]
    
    total_batches = (len(dialogos) + BATCH_SIZE - 1) // BATCH_SIZE
    
    for i in range(0, len(dialogos), BATCH_SIZE):
        batch = dialogos[i: i + BATCH_SIZE]
        batch_num = i // BATCH_SIZE + 1
        
        log.info(f"Procesando batch {batch_num}/{total_batches} ({len(batch)} conversaciones)...")
        
        prompt = construir_prompt_batch(batch)
        
        # Retry con backoff exponencial
        exito = False
        for intento in range(1, MAX_RETRIES + 1):
            try:
                response = client.models.generate_content(
                    model=GEMINI_MODEL,
                    contents=prompt,
                    config=types.GenerateContentConfig(
                        temperature=0.1,
                        max_output_tokens=4096,
                    )
                )
                
                texto_respuesta = response.text.strip()
                json_match = re.search(r'\{.*\}', texto_respuesta, re.DOTALL)
                if not json_match:
                    raise ValueError(f"No se encontro JSON en respuesta del batch {batch_num}")
                
                datos = json.loads(json_match.group())
                resultados_batch = datos.get("resultados", [])
                
                if len(resultados_batch) < len(batch):
                    log.warning(
                        f"Batch {batch_num}: esperaba {len(batch)} resultados, "
                        f"obtuvo {len(resultados_batch)}"
                    )
                
                resultados_todos.extend(resultados_batch)
                log.info(f"  OK Batch {batch_num}: {len(resultados_batch)} dolores extraidos")
                exito = True
                break
                
            except Exception as e:
                error_str = str(e)
                if "429" in error_str or "RESOURCE_EXHAUSTED" in error_str:
                    wait_time = 60 * intento
                    log.warning(f"  Rate limit (429). Esperando {wait_time}s...")
                    time.sleep(wait_time)
                else:
                    log.error(f"  ERROR en batch {batch_num} (intento {intento}): {e}")
                    if intento < MAX_RETRIES:
                        time.sleep(10)
                    else:
                        break
        
        if not exito:
            log.error(f"  FALLO DEFINITIVO en batch {batch_num} tras {MAX_RETRIES} intentos")
            batches_fallidos.append(batch_num)
            for conv_id, _ in batch:
                resultados_todos.append({
                    "conversation_id": conv_id,
                    "dolor": "Error al procesar"
                })
        
        if i + BATCH_SIZE < len(dialogos):
            time.sleep(SLEEP_BETWEEN_BATCHES)
    
    if batches_fallidos:
        log.warning(f"Batches con error: {batches_fallidos}")
    
    df_dolores = pd.DataFrame(resultados_todos)
    log.info(f"Extraccion completada. Total filas: {len(df_dolores)}")
    return df_dolores


# ─────────────────────────────────────────────
# PASO 4: CONSOLIDACION Y NORMALIZACION
# ─────────────────────────────────────────────

def normalizar_dolores_con_gemini(dolores_unicos: list, client) -> dict:
    """
    Usa Gemini para agrupar dolores similares bajo etiquetas canonicas.
    """
    # Excluimos explícitamente "Sin dolor expresado" para que no lo mezcle
    dolores_filtrados = [d for d in dolores_unicos if "sin dolor" not in d.lower()]
    dolores_texto = "\n".join([f"- {d}" for d in dolores_filtrados])
    
    prompt = (
        "Eres experto en analisis de clientes para servicios financieros en Argentina.\n\n"
        "Tienes esta lista de dolores/problemas expresados por clientes potenciales:\n\n"
        f"{dolores_texto}\n\n"
        "Agrupa los dolores similares bajo una etiqueta canonica comun en espanol rioplatense.\n"
        "Maximo 15-20 grupos distintos. Se especifico pero representativo.\n"
        "No uses jerga tecnica financiera, usa el lenguaje coloquial del cliente.\n\n"
        "Ejemplos de buenas etiquetas canonicas:\n"
        "- No llego a fin de mes\n"
        "- No se como ahorrar de forma consistente\n"
        "- Gastos mezclados entre negocio y vida personal\n"
        "- Quiero comprar mi primer vehiculo o propiedad\n"
        "- Ingresos variables o inestables\n\n"
        "Responde UNICAMENTE con JSON valido:\n"
        '{"mapeo": {"dolor original exacto": "etiqueta canonica", ...}}'
    )
    
    try:
        response = client.models.generate_content(
            model=GEMINI_MODEL,
            contents=prompt,
            config=types.GenerateContentConfig(
                temperature=0.1,
                max_output_tokens=8192,
            )
        )
        texto = response.text.strip()
        json_match = re.search(r'\{.*\}', texto, re.DOTALL)
        if not json_match:
            raise ValueError("No JSON en respuesta de normalizacion")
        datos = json.loads(json_match.group())
        mapeo = datos.get("mapeo", {})
        
        # Volver a mapear sin dolor expresado
        for d in dolores_unicos:
            if "sin dolor" in d.lower():
                mapeo[d] = "Sin dolor expresado"
                
        return mapeo
    except Exception as e:
        log.error(f"Error en normalizacion de dolores: {e}")
        return {d: d for d in dolores_unicos}


def consolidar_dolores(df_dolores: pd.DataFrame, api_key: str) -> pd.DataFrame:
    """
    Normaliza los dolores en categorias canonicas usando Gemini.
    """
    log.info("Consolidando y normalizando dolores...")
    
    df_dedup = (
        df_dolores
        .dropna(subset=["conversation_id", "dolor"])
        .drop_duplicates(subset=["conversation_id"])
        .copy()
    )
    log.info(f"Conversaciones unicas con dolor asignado: {len(df_dedup)}")
    
    dolores_unicos = df_dedup["dolor"].dropna().unique().tolist()
    log.info(f"Dolores unicos antes de normalizacion: {len(dolores_unicos)}")
    
    client = genai.Client(api_key=api_key)
    mapeo = normalizar_dolores_con_gemini(dolores_unicos, client)
    
    df_dedup = df_dedup.copy()
    df_dedup["dolor_canonico"] = df_dedup["dolor"].map(mapeo).fillna(df_dedup["dolor"])
    
    return df_dedup


# ─────────────────────────────────────────────
# PASO 5: REPORTE FINAL
# ─────────────────────────────────────────────

def generar_reporte_final(df_consolidado: pd.DataFrame) -> pd.DataFrame:
    """
    Genera el reporte final con las columnas solicitadas.
    Ordenado por frecuencia descendente (Pareto).
    """
    total_convs = df_consolidado["conversation_id"].nunique()
    
    reporte = (
        df_consolidado
        .groupby("dolor_canonico")["conversation_id"]
        .nunique()
        .reset_index()
        .rename(columns={
            "dolor_canonico": "Dolor comun",
            "conversation_id": "Cantidad de conversaciones unicas"
        })
        .sort_values("Cantidad de conversaciones unicas", ascending=False)
        .reset_index(drop=True)
    )
    
    reporte["Porcentaje de conversaciones unicas (%)"] = (
        reporte["Cantidad de conversaciones unicas"] / total_convs * 100
    ).round(1)
    
    reporte = reporte[[
        "Dolor comun",
        "Porcentaje de conversaciones unicas (%)",
        "Cantidad de conversaciones unicas"
    ]]
    
    return reporte


# ─────────────────────────────────────────────
# MAIN
# ─────────────────────────────────────────────

def main() -> None:
    log.info("=" * 60)
    log.info("ANALISIS DE DOLORES - TODAS LAS CONVERSACIONES (2026-08-20)")
    log.info("=" * 60)
    
    # 1. Cargar y filtrar
    df_todas, todas_ids = cargar_y_filtrar_todas(INPUT_CSV)
    
    # 2. Extraer dolores
    df_dolores = extraer_dolores_con_gemini(todas_ids, df_todas, GEMINI_API_KEY)
    
    # Guardar intermedio para trazabilidad
    df_dolores.to_csv(OUTPUT_POR_CONV, index=False, encoding="utf-8-sig")
    log.info(f"Dolores por conversacion guardados en: {OUTPUT_POR_CONV}")
    
    # 3. Consolidar y normalizar
    df_consolidado = consolidar_dolores(df_dolores, GEMINI_API_KEY)
    
    # 4. Reporte final
    reporte = generar_reporte_final(df_consolidado)
    
    # 5. Guardar
    reporte.to_csv(OUTPUT_DOLORES, index=False, encoding="utf-8-sig")
    log.info(f"Reporte final guardado en: {OUTPUT_DOLORES}")
    
    # 6. Mostrar resultado en consola
    log.info("\n" + "=" * 60)
    log.info("RESULTADO FINAL:")
    log.info("=" * 60)
    print(reporte.to_string(index=False))
    
    total_convs = df_consolidado["conversation_id"].nunique()
    log.info(f"\nTotal conversaciones analizadas: {total_convs}")
    log.info(f"Total categorias de dolor: {len(reporte)}")


if __name__ == "__main__":
    main()
