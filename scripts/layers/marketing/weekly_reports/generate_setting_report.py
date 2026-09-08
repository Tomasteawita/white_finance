"""
generate_setting_report.py
==========================
Genera el reporte semanal de métricas de setting:
- Msjs Outbound
- Msjs Inbound
- Seguimientos
- Agendas (DM)

Utiliza un enfoque híbrido: Python para calcular seguimientos basados en tiempo, 
y Gemini para interpretar Outbounds, Inbounds y Agendas.
"""

import os
import sys
import json
import time
import logging
import argparse
from datetime import datetime, timedelta
from pathlib import Path
import re

from follower_export import export_followers_href_to_csv
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

GEMINI_API_KEY = os.getenv("GEMINI_API_KEY", "")
GEMINI_MODEL = "gemini-flash-latest"
BATCH_SIZE = 15
MAX_RETRIES = 3
TOMAS_NAME_PATTERN = "Cueva"

# ─────────────────────────────────────────────
# UTILIDADES DE FECHA
# ─────────────────────────────────────────────

def get_date_str(timestamp_ms: int) -> str:
    """Convierte un timestamp en milisegundos a string YYYY-MM-DD."""
    return datetime.fromtimestamp(timestamp_ms / 1000.0).strftime('%Y-%m-%d')

def get_datetime_str(timestamp_ms: int) -> str:
    """Convierte un timestamp a string con hora para contexto de Gemini."""
    return datetime.fromtimestamp(timestamp_ms / 1000.0).strftime('%Y-%m-%d %H:%M:%S')

# ─────────────────────────────────────────────
# PASO 1: CARGA Y CONSOLIDACIÓN DE MENSAJES
# ─────────────────────────────────────────────

def cargar_conversaciones(base_dir: Path, start_date_str: str, end_date_str: str):
    """
    Lee todas las conversaciones, consolida los message_*.json, 
    y filtra las que tuvieron actividad en el período.
    Retorna un diccionario de conversaciones y una lista de métricas pre-calculadas (seguimientos).
    """
    start_date = datetime.strptime(start_date_str, "%Y-%m-%d").date()
    end_date = datetime.strptime(end_date_str, "%Y-%m-%d").date()
    
    if not base_dir.exists():
        log.error(f"El directorio no existe: {base_dir}")
        sys.exit(1)
        
    conversaciones = {}
    seguimientos_detectados = []
    
    dirs = [d for d in base_dir.iterdir() if d.is_dir()]
    log.info(f"Analizando {len(dirs)} directorios de conversaciones en {base_dir.name}...")
    
    for conv_dir in dirs:
        mensajes_conv = []
        # Cargar todos los message_*.json
        for msg_file in conv_dir.glob("message_*.json"):
            try:
                with open(msg_file, 'r', encoding='utf-8') as f:
                    data = json.load(f)
                    mensajes_conv.extend(data.get("messages", []))
            except Exception as e:
                log.warning(f"Error leyendo {msg_file}: {e}")
                
        if not mensajes_conv:
            continue
            
        # Ordenar mensajes por timestamp (ascendente = de más viejo a más nuevo)
        mensajes_conv.sort(key=lambda x: x.get("timestamp_ms", 0))
        
        # Evaluar actividad en el rango de fechas y calcular Seguimientos
        actividad_en_rango = False
        mensajes_filtrados = [] # Para Gemini
        
        last_sender_was_me = False
        last_timestamp = 0
        
        for i, msg in enumerate(mensajes_conv):
            ts = msg.get("timestamp_ms", 0)
            if ts == 0:
                continue
                
            msg_date = datetime.fromtimestamp(ts / 1000.0).date()
            sender = msg.get("sender_name", "")
            es_tomas = TOMAS_NAME_PATTERN in sender
            
            # Chequear Seguimiento (Regla: > 2 horas, yo mandé el anterior sin respuesta del cliente, y ahora mando yo de nuevo)
            # 2 horas = 7200000 ms
            if es_tomas:
                if last_sender_was_me and (ts - last_timestamp > 7200000):
                    # Solo contamos seguimientos si cayeron en nuestra ventana de fechas de reporte
                    if start_date <= msg_date <= end_date:
                        seguimientos_detectados.append({
                            "fecha": msg_date.strftime("%Y-%m-%d"),
                            "metrica": "Seguimientos",
                            "conversation_id": conv_dir.name
                        })
                last_sender_was_me = True
                last_timestamp = ts
            else:
                last_sender_was_me = False
                last_timestamp = ts

            if start_date <= msg_date <= end_date:
                actividad_en_rango = True
                
        if actividad_en_rango:
            # Para Gemini, pasamos toda la conversacion pero destacamos cuales estan en rango
            conversaciones[conv_dir.name] = mensajes_conv
            
    log.info(f"Conversaciones con actividad en rango: {len(conversaciones)}")
    log.info(f"Seguimientos detectados por Python: {len(seguimientos_detectados)}")
    
    return conversaciones, seguimientos_detectados
    

def cargar_seguidores(followers_file: Path, start_date_str: str, end_date_str: str) -> list:
    """Lee el JSON de seguidores y cuenta cuántos siguieron por día en el rango."""
    if not followers_file.exists():
        log.warning(f"No se encontró el archivo de seguidores: {followers_file}")
        return []
        
    start_date = datetime.strptime(start_date_str, "%Y-%m-%d").date()
    end_date = datetime.strptime(end_date_str, "%Y-%m-%d").date()
    
    try:
        with open(followers_file, 'r', encoding='utf-8') as f:
            data = json.load(f)
    except Exception as e:
        log.error(f"Error leyendo {followers_file}: {e}")
        return []
        
    # Extraer la lista de seguidores (manejar si es lista directo o un dict)
    lista_seguidores = []
    if isinstance(data, list):
        lista_seguidores = data
    elif isinstance(data, dict):
        # Buscar alguna key que contenga la lista (a menudo es un dict con listas)
        for k, v in data.items():
            if isinstance(v, list):
                lista_seguidores = v
                break
                
    seguidores_detectados = []
    for item in lista_seguidores:
        # El timestamp suele estar dentro de string_list_data[0]['timestamp'] en el formato de IG
        timestamp = 0
        if "string_list_data" in item and len(item["string_list_data"]) > 0:
            timestamp = item["string_list_data"][0].get("timestamp", 0)
            
        if timestamp:
            # IG a veces lo da en segundos y a veces en milisegundos.
            # 1e11 (año 5138) sirve para diferenciar entre segundos y ms
            if timestamp > 1e11: 
                timestamp = timestamp / 1000.0
            try:
                msg_date = datetime.fromtimestamp(timestamp).date()
                if start_date <= msg_date <= end_date:
                    seguidores_detectados.append({
                        "fecha": msg_date.strftime("%Y-%m-%d"),
                        "metrica": "Seguidores",
                        "conversation_id": "follower" # Usamos esto para que pivot cuente un +1 por cada uno
                    })
            except Exception as e:
                pass
                
    log.info(f"Seguidores detectados en el rango: {len(seguidores_detectados)}")
    return seguidores_detectados

# ─────────────────────────────────────────────
# PASO 2: PROCESAMIENTO CON GEMINI
# ─────────────────────────────────────────────

def construir_prompt_batch(conversaciones: list, start_date_str: str, end_date_str: str) -> str:
    """Construye el prompt para un batch de conversaciones."""
    conv_texts = []
    
    # Filtros de patrones de Instagram auto-generados que debemos ignorar
    IGNORAR_PATRONES = [
        "you sent an attachment", "sent an attachment",
        "reacted", "replied to an ad", "reminder:", "view ad",
        "https://", "http://"
    ]
    
    for conv_id, msgs in conversaciones:
        lines = []
        
        for i, msg in enumerate(msgs):
            ts = msg.get("timestamp_ms", 0)
            texto = str(msg.get("content", "")).strip()
            
            if not texto or texto.lower() == "nan":
                continue
                
            if any(p in texto.lower() for p in IGNORAR_PATRONES):
                continue
                
            es_tomas = TOMAS_NAME_PATTERN in str(msg.get("sender_name", ""))
            rol = "TOMAS" if es_tomas else "CLIENTE"
            dt_str = get_datetime_str(ts)
            date_str = get_date_str(ts)
            
            # Solo enviamos el mensaje si está en el rango de fechas, o si es el primer mensaje histórico
            if i == 0:
                lines.append(f"[{dt_str}] (Primer msg histórico) [{rol}]: {texto}")
            elif start_date_str <= date_str <= end_date_str:
                lines.append(f"[{dt_str}] [{rol}]: {texto}")
                
        if lines:
            conv_texts.append(
                f"--- CONVERSACION ID: {conv_id} ---\n" + "\n".join(lines) + "\n--- FIN CONVERSACION ---"
            )
            
    batch_text = "\n\n".join(conv_texts)
    
    prompt = (
        "Eres un asistente analítico experto en ventas (setting) en Instagram.\n"
        f"Analiza las siguientes conversaciones, enfocándote en los mensajes enviados entre {start_date_str} y {end_date_str}.\n\n"
        "Debes extraer las siguientes métricas y sus fechas exactas (YYYY-MM-DD):\n"
        "1. Msjs Outbound: Cuando TOMAS envía el PRIMER mensaje de la conversación de forma fría.\n"
        "   - Solo si el 'Primer msg histórico' fue enviado por TOMAS y cae dentro de las fechas a analizar.\n"
        "2. Msj Inbound: Cuando el CLIENTE inicia una nueva conversación con TOMAS.\n"
        "   - Solo si el 'Primer msg histórico' fue enviado por el CLIENTE y cae dentro de las fechas a analizar.\n"
        "3. Agendas (DM): Cuando el CLIENTE proporciona explícitamente un email (ej. @gmail.com) o un número de WhatsApp.\n"
        "   - Debes identificar en qué fecha exacta dentro del período analizado el cliente pasó el contacto.\n\n"
        "REGLAS:\n"
        "- Respuestas a historias no son Outbounds fríos. Si ves que el texto parece una reacción a una historia, no lo cuentes como Outbound.\n"
        "- Responde ÚNICAMENTE con JSON válido en este formato estricto:\n"
        '{"resultados": [{"conversation_id": "ID", "metrica": "Msjs Outbound", "fecha": "YYYY-MM-DD"}, ...]}\n'
        "- Si una conversación no tiene ninguna de estas métricas, no la incluyas en la lista.\n\n"
        "CONVERSACIONES:\n\n"
        f"{batch_text}"
    )
    return prompt

def analizar_con_gemini(conversaciones: dict, start_date_str: str, end_date_str: str, api_key: str) -> list:
    if not api_key:
        raise ValueError("GEMINI_API_KEY no configurada.")
        
    client = genai.Client(api_key=api_key)
    log.info(f"Iniciando extracción Gemini con {len(conversaciones)} conversaciones en rango.")
    
    resultados_gemini = []
    items = list(conversaciones.items())
    total_batches = (len(items) + BATCH_SIZE - 1) // BATCH_SIZE
    
    for i in range(0, len(items), BATCH_SIZE):
        batch = items[i: i + BATCH_SIZE]
        batch_num = i // BATCH_SIZE + 1
        
        log.info(f"Procesando batch {batch_num}/{total_batches} ({len(batch)} conversaciones)...")
        prompt = construir_prompt_batch(batch, start_date_str, end_date_str)
        
        exito = False
        for intento in range(1, MAX_RETRIES + 1):
            try:
                response = client.models.generate_content(
                    model=GEMINI_MODEL,
                    contents=prompt,
                    config=types.GenerateContentConfig(
                        temperature=0.0,
                        max_output_tokens=4096,
                    )
                )
                
                texto = response.text.strip()
                json_match = re.search(r'\{.*\}', texto, re.DOTALL)
                if not json_match:
                    raise ValueError("No se encontro JSON en la respuesta.")
                    
                datos = json.loads(json_match.group())
                resultados_gemini.extend(datos.get("resultados", []))
                
                log.info(f"  OK Batch {batch_num}: {len(datos.get('resultados', []))} metricas extraidas.")
                exito = True
                time.sleep(2) # Respetar rate limits
                break
            except Exception as e:
                error_str = str(e)
                log.warning(f"Error en batch {batch_num}, intento {intento}: {error_str}")
                if "429" in error_str or "RESOURCE_EXHAUSTED" in error_str:
                    time.sleep(30 * intento)
                else:
                    time.sleep(5)
                    
        if not exito:
            log.error(f"Fallo definitivo en batch {batch_num}.")
            
    return resultados_gemini

# ─────────────────────────────────────────────
# PASO 3: REPORTE FINAL
# ─────────────────────────────────────────────

def generar_reporte_csv(resultados_totales: list, output_path: Path):
    if not resultados_totales:
        log.warning("No se encontraron métricas para exportar.")
        return
        
    df = pd.DataFrame(resultados_totales)
    # df tiene columnas: conversation_id, metrica, fecha
    
    # Agrupar por fecha y metrica
    pivot = df.pivot_table(
        index='fecha', 
        columns='metrica', 
        values='conversation_id', 
        aggfunc='count', 
        fill_value=0
    ).reset_index()
    
    # Asegurar que existan todas las columnas
    metricas_esperadas = ["Seguidores", "Msjs Outbound", "Msj Inbound", "Seguimientos", "Agendas (DM)"]
    for m in metricas_esperadas:
        if m not in pivot.columns:
            pivot[m] = 0
            
    # Ordenar columnas
    columnas = ['fecha'] + metricas_esperadas
    pivot = pivot[columnas]
    pivot = pivot.sort_values('fecha')
    
    pivot.to_csv(output_path, index=False, encoding='utf-8-sig')
    log.info(f"\nReporte guardado exitosamente en: {output_path}")
    
    # Imprimir en consola
    print("\n" + "="*50)
    print("RESUMEN SEMANAL DE SETTING")
    print("="*50)
    print(pivot.to_string(index=False))
    print("="*50)
    
    # Totales
    print("\nTOTALES DEL PERÍODO:")
    for m in metricas_esperadas:
        print(f"- {m}: {pivot[m].sum()}")

# ─────────────────────────────────────────────
# MAIN
# ─────────────────────────────────────────────

def main():
    parser = argparse.ArgumentParser(description="Generador de Reporte Semanal de Setting")
    parser.add_argument("--start", type=str, help="Fecha de inicio (YYYY-MM-DD)")
    parser.add_argument("--end", type=str, help="Fecha de fin (YYYY-MM-DD)")
    parser.add_argument("--process-date", type=str, help="Fecha de procesamiento de datos (YYYY-MM-DD)")
    
    args = parser.parse_args()
    
    print("="*60)
    print(" GENERADOR DE REPORTE DE SETTING (HÍBRIDO PYTHON + GEMINI)")
    print("="*60)
    
    start_date_str = args.start or input("Ingrese fecha de inicio (YYYY-MM-DD): ").strip()
    end_date_str = args.end or input("Ingrese fecha de fin (YYYY-MM-DD): ").strip()
    process_date_str = args.process_date or input("Ingrese fecha del reporte/descarga (YYYY-MM-DD): ").strip()
    
    # Limpiar proceso
    process_date_clean = process_date_str.replace("-", "")
    
    base_dir = Path(f"D:/DatosDeMercado/marketing_data/instagram_{process_date_clean}/your_instagram_activity/messages/inbox")
    followers_file = Path(f"D:/DatosDeMercado/marketing_data/instagram_{process_date_clean}/connections/followers_and_following/followers_1.json")
    output_dir = Path(f"D:/DatosDeMercado/marketing_data/instagram_{process_date_clean}/processed")
    output_dir.mkdir(parents=True, exist_ok=True)
    
    output_csv = output_dir / f"setting_weekly_report_{start_date_str}_{end_date_str}.csv"
    output_followers_csv = output_dir / f"followers_href_{start_date_str}_{end_date_str}.csv"
    
    # 1. Cargar y procesar lógica Python (Conversaciones y Seguidores)
    conversaciones, seguimientos = cargar_conversaciones(base_dir, start_date_str, end_date_str)
    seguidores = cargar_seguidores(followers_file, start_date_str, end_date_str)
    
    # Exportar listado de href de seguidores en el rango
    export_followers_href_to_csv(followers_file, start_date_str, end_date_str, output_followers_csv)
    
    # 2. Procesar con Gemini (Outbounds, Inbounds, Agendas)
    resultados_gemini = analizar_con_gemini(conversaciones, start_date_str, end_date_str, GEMINI_API_KEY)
    
    # 3. Unir resultados
    resultados_totales = seguimientos + resultados_gemini + seguidores
    
    # 4. Generar reporte
    generar_reporte_csv(resultados_totales, output_csv)

if __name__ == "__main__":
    main()
