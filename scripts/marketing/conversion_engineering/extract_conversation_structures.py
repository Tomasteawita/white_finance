import os
import json
import logging
import time
import re
from pathlib import Path
import pandas as pd
from google import genai
from google.genai import types
from dotenv import load_dotenv

load_dotenv()
log = logging.getLogger(__name__)

GEMINI_API_KEY = os.getenv("GEMINI_API_KEY", "")
GEMINI_MODEL = "gemini-flash-latest"
TOMAS_NAME_PATTERN = "Cueva"

def reconstruir_dialogo(msgs: list) -> str:
    msgs.sort(key=lambda x: x.get("timestamp_ms", 0))
    IGNORAR_PATRONES = [
        "you sent an attachment", "sent an attachment",
        "reacted", "replied to an ad", "reminder:", "view ad",
        "https://", "http://"
    ]
    
    lines = []
    for row in msgs:
        texto = row.get("full_text", "").strip()
        if not texto: continue
            
        texto_lower = texto.lower()
        if any(p in texto_lower for p in IGNORAR_PATRONES) and len(texto) < 40:
            continue
        
        es_tomas = TOMAS_NAME_PATTERN in str(row.get("sender_name", ""))
        rol = "TOMAS" if es_tomas else "CLIENTE"
        lines.append(f"[{rol}]: {texto}")
    
    return "\n".join(lines) if lines else "[CONVERSACION SIN CONTENIDO TEXTUAL]"

def construir_prompt(dialogo: str) -> str:
    return (
        "Eres un experto en ventas e ingeniería de conversión B2B/B2C en Argentina.\n"
        "A continuación, verás una conversación exitosa de Instagram DMs entre un asesor financiero (TOMAS) "
        "y un potencial cliente (CLIENTE). Esta charla fue exitosa porque el cliente terminó entregando su "
        "contacto (teléfono, email) o agendando una reunión.\n\n"
        "Tu objetivo es analizar la anatomía de esta venta y extraer las variables clave que llevaron al éxito.\n\n"
        "REGLAS OBLIGATORIAS:\n"
        "1. Debes extraer las FASES estructurales por las que pasó la conversación en orden cronológico "
        "(ej. ['Rompehielo', 'Identificación de dolor', 'Aporte de valor', 'Transición al cierre', 'Captura de contacto']).\n"
        "2. Identifica las PREGUNTAS CLAVE que hizo TOMAS que destrabaron la confianza o hicieron avanzar el embudo.\n"
        "3. Identifica las RESPUESTAS CLAVE del CLIENTE (señales de compra, revelación de dolores profundos).\n\n"
        "Responde UNICAMENTE con un JSON válido usando el siguiente esquema exacto:\n"
        "{\n"
        '  "estructura_fases": ["fase 1", "fase 2", ...],\n'
        '  "preguntas_clave_tomas": ["pregunta exacta 1", "pregunta exacta 2", ...],\n'
        '  "respuestas_clave_cliente": ["respuesta exacta 1", "respuesta exacta 2", ...]\n'
        "}\n\n"
        "CONVERSACIÓN:\n"
        f"{dialogo}\n"
    )

def extraer_estructura(client: genai.Client, dialogo: str) -> dict:
    prompt = construir_prompt(dialogo)
    for intento in range(3):
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
            if json_match:
                return json.loads(json_match.group())
        except Exception as e:
            log.warning(f"Error con Gemini (intento {intento+1}): {e}")
            time.sleep(5)
    return {}

def run(process_date: str):
    log.info("Fase B: Extracción Profunda de Estructuras (LLM)")
    if not GEMINI_API_KEY:
        log.error("Falta GEMINI_API_KEY en el entorno")
        return
        
    input_jsonl = Path(f"D:/DatosDeMercado/marketing_data/instagram_{process_date}/processed/chat_corpus_nlp.jsonl")
    reports_dir = Path(r"C:\Users\User\white_finance\reports\marketing")
    input_ids = reports_dir / "conversaciones_exitosas_ids.json"
    output_json = reports_dir / "conversaciones_estructuradas.json"
    output_csv = reports_dir / "conversaciones_estructuradas.csv"
    
    if not input_ids.exists() or not input_jsonl.exists():
        log.error("Faltan archivos de entrada (IDs o JSONL).")
        return
        
    with open(input_ids, "r", encoding="utf-8") as f:
        exitosos_ids = set(json.load(f))
        
    log.info(f"Se procesarán {len(exitosos_ids)} conversaciones exitosas.")
    
    conversaciones = {cid: [] for cid in exitosos_ids}
    with open(input_jsonl, "r", encoding="utf-8") as f:
        for line in f:
            if not line.strip(): continue
            record = json.loads(line)
            cid = record["conversation_id"]
            if cid in exitosos_ids:
                conversaciones[cid].append(record)
                
    client = genai.Client(api_key=GEMINI_API_KEY)
    resultados = []
    
    for i, (cid, msgs) in enumerate(conversaciones.items(), 1):
        log.info(f"Analizando conversación {i}/{len(conversaciones)}: {cid}")
        dialogo = reconstruir_dialogo(msgs)
        datos_extraidos = extraer_estructura(client, dialogo)
        if datos_extraidos:
            datos_extraidos["conversation_id"] = cid
            resultados.append(datos_extraidos)
        time.sleep(3)
        
    with open(output_json, "w", encoding="utf-8") as f:
        json.dump(resultados, f, indent=4, ensure_ascii=False)
        
    filas_csv = []
    for r in resultados:
        filas_csv.append({
            "conversation_id": r.get("conversation_id"),
            "estructura_fases": " | ".join(r.get("estructura_fases", [])),
            "preguntas_clave_tomas": " | ".join(r.get("preguntas_clave_tomas", [])),
            "respuestas_clave_cliente": " | ".join(r.get("respuestas_clave_cliente", []))
        })
        
    df = pd.DataFrame(filas_csv)
    if not df.empty:
        df.to_csv(output_csv, index=False, encoding="utf-8-sig")
    log.info(f"Extracción finalizada. JSON={output_json.name}, CSV={output_csv.name}")
