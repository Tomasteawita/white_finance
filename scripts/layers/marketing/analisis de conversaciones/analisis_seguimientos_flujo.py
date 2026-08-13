"""
analisis_seguimientos_flujo.py
==============================
Analiza las transcripciones de Instagram DMs para identificar qué estrategias
de seguimiento funcionaron mejor en conversaciones que finalmente lograron
el objetivo de agendar una llamada (se envió enlace de Calendly).

Reglas de Negocio:
1. Conversaciones iniciadas por el cliente con "FLUJO" (case-insensitive).
2. Se enviaron enlaces de Calendly ("calendly.com") posteriormente en la conversación.
3. Un seguimiento se define como un mensaje o ráfaga de mensajes de Tomas hacia el cliente,
   cuando el último mensaje de la conversación fue TAMBIÉN de Tomas, y ha pasado
   un lapso mayor a 20 minutos (inactividad / dejado en visto).

Pipeline:
1. Filtrado de datos (FLUJO + Calendly).
2. Algoritmo secuencial cronológico para extraer seguimientos previos al primer Calendly.
3. Clasificación de estrategias de seguimiento usando Gemini Flash.
4. Consolidación de estrategias y reporte basado en frecuencia (Pareto).
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

INPUT_CSV = Path("D:/DatosDeMercado/marketing_data/instagram_20260802/processed/instagram_audio_transcripts.csv")
OUTPUT_DIR = Path("D:/DatosDeMercado/marketing_data/instagram_20260802/processed")
OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

OUTPUT_SEGUIMIENTOS = OUTPUT_DIR / "seguimientos_exitosos_consolidados.csv"
OUTPUT_POR_CONV = OUTPUT_DIR / "seguimientos_por_conversacion.csv"

GEMINI_API_KEY = os.getenv("GEMINI_API_KEY", "")
GEMINI_MODEL = "gemini-flash-latest"
BATCH_SIZE = 20                      
SLEEP_BETWEEN_BATCHES = 3            
MAX_RETRIES = 3                      

TOMAS_NAME_PATTERN = "Cueva"
CALENDLY_URL = "calendly.com"


# ─────────────────────────────────────────────
# PASO 1: CARGA Y FILTRADO
# ─────────────────────────────────────────────

def cargar_dataset_completo(csv_path: Path) -> tuple[pd.DataFrame, list[str]]:
    """
    Carga todas las conversaciones del dataset sin filtrar.
    """
    log.info(f"Cargando CSV: {csv_path}")
    df = pd.read_csv(csv_path, encoding="latin-1")
    convs = df["conversation_id"].dropna().unique().tolist()
    log.info(f"Total conversaciones en el dataset: {len(convs)}")
    return df, convs


# ─────────────────────────────────────────────
# PASO 2: EXTRACCIÓN DE SEGUIMIENTOS
# ─────────────────────────────────────────────

def extraer_seguimientos_historicos(df_completo: pd.DataFrame, convs: list[str]) -> pd.DataFrame:
    """
    Recorre cronológicamente cada conversación para encontrar seguimientos de Tomas.
    """
    seguimientos = []
    
    IGNORAR_PATRONES = [
        "you sent an attachment", "sent an attachment",
        "reacted", "replied to an ad", "reminder:", "view ad",
        "https://", "http://" # Omitimos links genericos, pero mantenemos texto si lo hay
    ]
    
    for conv_id in convs:
        conv = df_completo[df_completo["conversation_id"] == conv_id].sort_values("timestamp_ms")
        
        last_sender = None
        last_tomas_ts = None
        
        is_in_followup = False
        current_followup_msgs = []
        history_context = []
        context_for_current_followup = ""
        
        for _, row in conv.iterrows():
            texto = str(row.get("full_text", "")).strip()
            if not texto or texto.lower() == "nan":
                continue
                
            es_tomas = TOMAS_NAME_PATTERN in str(row["sender_name"])
            sender = "TOMAS" if es_tomas else "CLIENTE"
            ts = row["timestamp_ms"]
            
            # Limpiar patrones de adjuntos sin valor de texto
            texto_lower = texto.lower()
            if any(p in texto_lower for p in IGNORAR_PATRONES) and len(texto) < 40:
                # Solo omitimos si es un attachment puro y corto
                pass
            else:
                texto_display = texto[:300] + "..." if len(texto) > 300 else texto
                
                if sender == "TOMAS":
                    if last_sender == "TOMAS" and last_tomas_ts is not None:
                        delta_ms = ts - last_tomas_ts
                        if delta_ms > (20 * 60 * 1000): # 20 minutos
                            # Cierra el seguimiento anterior si había uno
                            if is_in_followup and current_followup_msgs:
                                seguimientos.append({
                                    "conversation_id": conv_id,
                                    "contexto": context_for_current_followup,
                                    "followup_text": " | ".join(current_followup_msgs)
                                })
                                current_followup_msgs = []
                            
                            is_in_followup = True
                            current_followup_msgs.append(texto_display)
                            # Guardar contexto de hasta 4 mensajes previos
                            context_for_current_followup = "\n".join(history_context[-4:])
                        else:
                            # Es parte de la misma ráfaga
                            if is_in_followup:
                                current_followup_msgs.append(texto_display)
                    else:
                        # Tomas responde al cliente o primer mensaje de Tomas
                        if is_in_followup and current_followup_msgs:
                            seguimientos.append({
                                "conversation_id": conv_id,
                                "contexto": context_for_current_followup,
                                "followup_text": " | ".join(current_followup_msgs)
                            })
                        current_followup_msgs = []
                        is_in_followup = False
                    
                    last_tomas_ts = ts
                    
                else: # CLIENTE
                    if is_in_followup and current_followup_msgs:
                        seguimientos.append({
                            "conversation_id": conv_id,
                            "contexto": context_for_current_followup,
                            "followup_text": " | ".join(current_followup_msgs)
                        })
                    current_followup_msgs = []
                    is_in_followup = False
                    
                last_sender = sender
                history_context.append(f"[{sender}]: {texto_display}")
                
        # Cierre final
        if is_in_followup and current_followup_msgs:
            seguimientos.append({
                "conversation_id": conv_id,
                "contexto": context_for_current_followup,
                "followup_text": " | ".join(current_followup_msgs)
            })
            
    df_seguimientos = pd.DataFrame(seguimientos)
    if not df_seguimientos.empty:
        df_seguimientos["id"] = ["seg_" + str(i+1) for i in range(len(df_seguimientos))]
    log.info(f"Total seguimientos extraídos: {len(df_seguimientos)}")
    return df_seguimientos


# ─────────────────────────────────────────────
# PASO 3: ANÁLISIS DE ESTRATEGIAS VIA GEMINI
# ─────────────────────────────────────────────

def construir_prompt_seguimientos(batch: pd.DataFrame) -> str:
    batch_text = ""
    for _, row in batch.iterrows():
        batch_text += f"--- SEGUIMIENTO ID: {row['id']} ---\n"
        batch_text += f"Contexto (mensajes previos):\n{row['contexto']}\n\n"
        batch_text += f"Mensaje(s) de Seguimiento de TOMAS:\n{row['followup_text']}\n"
        batch_text += "--- FIN SEGUIMIENTO ---\n\n"
        
    prompt = (
        "Eres un experto analista en ventas y marketing digital para servicios financieros.\n"
        "A continuación verás varios mensajes de 'seguimiento' que un asesor financiero (TOMAS) "
        "envió a potenciales clientes por Instagram luego de que estos lo dejaran de responder (gap > 20 mins).\n\n"
        "Tu tarea es analizar CADA SEGUIMIENTO y determinar la ESTRATEGIA PRINCIPAL que usó TOMAS para reactivar la charla.\n\n"
        "REGLAS OBLIGATORIAS:\n"
        "1. Responde con un máximo de 8-10 palabras por estrategia.\n"
        "2. Usa español rioplatense (ej. 'Pregunta directa sobre el dolor', 'Aporte de valor en audio', 'Empujar urgencia', 'Pregunta sobre continuidad').\n"
        "3. Se específico pero generalizable (sin nombres propios ni montos).\n"
        "4. Un solo tipo de estrategia por seguimiento.\n\n"
        "Responde UNICAMENTE con JSON válido en este formato:\n"
        '{"resultados": [{"id": "ID_AQUI", "estrategia": "descripción de estrategia"}, ...]}\n\n'
        "SEGUIMIENTOS A ANALIZAR:\n\n"
        f"{batch_text}"
    )
    return prompt


def clasificar_estrategias(df_seguimientos: pd.DataFrame, api_key: str) -> pd.DataFrame:
    if df_seguimientos.empty:
        return df_seguimientos
        
    client = genai.Client(api_key=api_key)
    log.info(f"Iniciando clasificación con Gemini... Batches de {BATCH_SIZE}")
    
    resultados_todos = []
    
    for i in range(0, len(df_seguimientos), BATCH_SIZE):
        batch = df_seguimientos.iloc[i : i + BATCH_SIZE]
        batch_num = i // BATCH_SIZE + 1
        
        log.info(f"Procesando batch {batch_num} ({len(batch)} seguimientos)...")
        prompt = construir_prompt_seguimientos(batch)
        
        exito = False
        for intento in range(1, MAX_RETRIES + 1):
            try:
                response = client.models.generate_content(
                    model=GEMINI_MODEL,
                    contents=prompt,
                    config=types.GenerateContentConfig(
                        temperature=0.2,
                        max_output_tokens=4096,
                    )
                )
                texto = response.text.strip()
                json_match = re.search(r'\{.*\}', texto, re.DOTALL)
                if not json_match:
                    raise ValueError(f"Sin JSON en respuesta batch {batch_num}")
                    
                datos = json.loads(json_match.group())
                res_batch = datos.get("resultados", [])
                resultados_todos.extend(res_batch)
                
                log.info(f"  OK Batch {batch_num}: {len(res_batch)} analizados")
                exito = True
                break
                
            except Exception as e:
                error_str = str(e)
                if "429" in error_str or "RESOURCE_EXHAUSTED" in error_str:
                    wait_time = 60 * intento
                    log.warning(f"  Rate limit 429. Esperando {wait_time}s (Intento {intento}/{MAX_RETRIES})")
                    time.sleep(wait_time)
                else:
                    log.error(f"  ERROR batch {batch_num} (Intento {intento}): {e}")
                    if intento < MAX_RETRIES:
                        time.sleep(10)
                    else:
                        break
                        
        if not exito:
            log.error(f"  Fallo definitivo en batch {batch_num}")
            
        time.sleep(SLEEP_BETWEEN_BATCHES)
        
    df_res = pd.DataFrame(resultados_todos)
    if not df_res.empty:
        df_final = df_seguimientos.merge(df_res, on="id", how="left")
        return df_final
    return df_seguimientos

# ─────────────────────────────────────────────
# PASO 4: NORMALIZACIÓN
# ─────────────────────────────────────────────

def normalizar_estrategias(estrategias: list, client) -> dict:
    texto_est = "\n".join([f"- {e}" for e in estrategias if str(e) != 'nan'])
    
    prompt = (
        "Eres un experto en ventas y análisis de procesos comerciales.\n"
        "Tienes esta lista de estrategias de seguimiento que usó un asesor financiero:\n\n"
        f"{texto_est}\n\n"
        "Agrupa las estrategias similares bajo una categoría canónica común (máximo 8-10 categorías).\n"
        "Ejemplos de categorías canónicas:\n"
        "- Pregunta directa de interés/continuidad\n"
        "- Aporte de valor (audios, links, consejos)\n"
        "- Push de escasez/urgencia de cupos\n"
        "- Seguimiento amistoso / 'ping'\n"
        "- Manejo de objeción de tiempo/dinero\n\n"
        "Responde UNICAMENTE con JSON válido:\n"
        '{"mapeo": {"estrategia original": "categoría canónica", ...}}'
    )
    try:
        response = client.models.generate_content(
            model=GEMINI_MODEL,
            contents=prompt,
            config=types.GenerateContentConfig(temperature=0.1)
        )
        json_match = re.search(r'\{.*\}', response.text.strip(), re.DOTALL)
        if json_match:
            return json.loads(json_match.group()).get("mapeo", {})
    except Exception as e:
        log.error(f"Error en normalización: {e}")
    return {e: e for e in estrategias}


# ─────────────────────────────────────────────
# MAIN
# ─────────────────────────────────────────────

def main():
    log.info("=" * 60)
    log.info("ANÁLISIS DE SEGUIMIENTOS - DATASET COMPLETO")
    log.info("=" * 60)
    
    df_completo, convs = cargar_dataset_completo(INPUT_CSV)
    df_seguimientos = extraer_seguimientos_historicos(df_completo, convs)
    
    if df_seguimientos.empty:
        log.warning("No se encontraron seguimientos en el dataset.")
        return
        
    df_clasificado = clasificar_estrategias(df_seguimientos, GEMINI_API_KEY)
    
    # Guardar detalle
    df_clasificado.to_csv(OUTPUT_POR_CONV, index=False, encoding="utf-8-sig")
    log.info(f"Detalle guardado en: {OUTPUT_POR_CONV}")
    
    # Normalizar
    if "estrategia" in df_clasificado.columns:
        unicas = df_clasificado["estrategia"].dropna().unique().tolist()
        client = genai.Client(api_key=GEMINI_API_KEY)
        mapeo = normalizar_estrategias(unicas, client)
        
        df_clasificado["estrategia_canonica"] = df_clasificado["estrategia"].map(mapeo).fillna(df_clasificado["estrategia"])
        
        # Reporte
        total_convs = df_completo["conversation_id"].nunique()
        total_segs = len(df_clasificado)
        
        reporte = (
            df_clasificado
            .groupby("estrategia_canonica")["id"]
            .count()
            .reset_index()
            .rename(columns={"estrategia_canonica": "Estrategia de Seguimiento", "id": "Cantidad de Uso"})
            .sort_values("Cantidad de Uso", ascending=False)
        )
        
        reporte["Porcentaje del Total (%)"] = (reporte["Cantidad de Uso"] / total_segs * 100).round(1)
        
        reporte.to_csv(OUTPUT_SEGUIMIENTOS, index=False, encoding="utf-8-sig")
        log.info(f"Reporte consolidado guardado en: {OUTPUT_SEGUIMIENTOS}")
        
        print("\n" + "="*60)
        print("REPORTE DE ESTRATEGIAS DE SEGUIMIENTO (PARETO):")
        print("="*60)
        print(reporte.to_string(index=False))
        
        print(f"\nConversaciones totales analizadas: {total_convs}")
        print(f"Total seguimientos enviados en todo el dataset: {total_segs}")
    else:
        log.error("No se pudo extraer estrategias de Gemini.")

if __name__ == "__main__":
    main()
