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

def consolidar_con_gemini(client: genai.Client, elementos: list[str], tipo: str) -> dict:
    elementos_str = "\n".join([f"- {e}" for e in elementos])
    
    contexto = ""
    if tipo == "estructuras":
        contexto = "Son secuencias o fases de una conversación de ventas."
    elif tipo == "preguntas":
        contexto = "Son preguntas clave hechas por un asesor financiero (Tomás) para generar confianza y cerrar."
    elif tipo == "respuestas":
        contexto = "Son respuestas de potenciales clientes que demuestran interés, dolor o decisión de avanzar."
        
    prompt = (
        f"Eres experto en análisis de ventas y procesos comerciales.\n\n"
        f"Tengo una lista de '{tipo}' extraídas de conversaciones de ventas exitosas en Instagram DMs:\n\n"
        f"{elementos_str}\n\n"
        f"Contexto: {contexto}\n\n"
        "Tu tarea es agrupar los elementos semánticamente similares bajo una ETIQUETA CANÓNICA común (máximo 10 etiquetas en total).\n"
        "Usa español rioplatense y términos claros de ventas (ej. 'Rompehielo y Rapport', 'Pregunta sobre capacidad de ahorro', 'Confirmación de presupuesto').\n\n"
        "Responde ÚNICAMENTE con un JSON válido en el siguiente formato:\n"
        "{\n"
        '  "mapeo": {\n'
        '    "elemento original 1": "Etiqueta Canónica A",\n'
        '    "elemento original 2": "Etiqueta Canónica A",\n'
        '    "elemento original 3": "Etiqueta Canónica B"\n'
        '  }\n'
        "}\n"
    )

    for intento in range(3):
        try:
            response = client.models.generate_content(
                model=GEMINI_MODEL,
                contents=prompt,
                config=types.GenerateContentConfig(
                    temperature=0.1,
                    max_output_tokens=4096,
                )
            )
            texto = response.text.strip()
            json_match = re.search(r'\{.*\}', texto, re.DOTALL)
            if json_match:
                datos = json.loads(json_match.group())
                return datos.get("mapeo", {})
        except Exception as e:
            log.warning(f"Error con Gemini (intento {intento+1}): {e}")
            time.sleep(5)
            
    return {e: e for e in elementos}

def generar_pareto(df: pd.DataFrame, columna: str, output_path: Path):
    if df.empty: return
    total = len(df)
    reporte = (
        df.groupby(columna)
        .size()
        .reset_index(name='Frecuencia Absoluta')
        .sort_values('Frecuencia Absoluta', ascending=False)
        .reset_index(drop=True)
    )
    reporte["Porcentaje (%)"] = (reporte["Frecuencia Absoluta"] / total * 100).round(1)
    reporte.to_csv(output_path, index=False, encoding="utf-8-sig")
    return reporte

def run():
    log.info("Fase C: Consolidación y Pareto")
    
    reports_dir = Path(r"C:\Users\User\white_finance\reports\marketing")
    input_json = reports_dir / "conversaciones_estructuradas.json"
    out_est = reports_dir / "pareto_estructuras.csv"
    out_preg = reports_dir / "pareto_preguntas_tomas.csv"
    out_resp = reports_dir / "pareto_respuestas_cliente.csv"
    
    if not input_json.exists():
        log.error(f"Falta el archivo: {input_json}")
        return
        
    with open(input_json, "r", encoding="utf-8") as f:
        datos = json.load(f)
        
    if not datos:
        log.warning("No hay datos extraídos para consolidar.")
        return
        
    todas_estructuras = []
    todas_preguntas = []
    todas_respuestas = []
    
    for d in datos:
        todas_estructuras.extend(d.get("estructura_fases", []))
        todas_preguntas.extend(d.get("preguntas_clave_tomas", []))
        todas_respuestas.extend(d.get("respuestas_clave_cliente", []))
        
    todas_estructuras = list(set(todas_estructuras))
    todas_preguntas = list(set(todas_preguntas))
    todas_respuestas = list(set(todas_respuestas))
    
    client = genai.Client(api_key=GEMINI_API_KEY)
    
    log.info(f"Consolidando {len(todas_estructuras)} estructuras...")
    mapeo_estructuras = consolidar_con_gemini(client, todas_estructuras, "estructuras")
    
    log.info(f"Consolidando {len(todas_preguntas)} preguntas de Tomás...")
    mapeo_preguntas = consolidar_con_gemini(client, todas_preguntas, "preguntas")
    
    log.info(f"Consolidando {len(todas_respuestas)} respuestas del prospecto...")
    mapeo_respuestas = consolidar_con_gemini(client, todas_respuestas, "respuestas")
    
    rows_est = []
    rows_preg = []
    rows_resp = []
    
    for d in datos:
        for e in d.get("estructura_fases", []):
            rows_est.append({"conversation_id": d["conversation_id"], "Estructura Original": e, "Estructura Canónica": mapeo_estructuras.get(e, e)})
        for p in d.get("preguntas_clave_tomas", []):
            rows_preg.append({"conversation_id": d["conversation_id"], "Pregunta Original": p, "Pregunta Canónica": mapeo_preguntas.get(p, p)})
        for r in d.get("respuestas_clave_cliente", []):
            rows_resp.append({"conversation_id": d["conversation_id"], "Respuesta Original": r, "Respuesta Canónica": mapeo_respuestas.get(r, r)})
            
    df_est = pd.DataFrame(rows_est)
    df_preg = pd.DataFrame(rows_preg)
    df_resp = pd.DataFrame(rows_resp)
    
    generar_pareto(df_est, "Estructura Canónica", out_est)
    generar_pareto(df_preg, "Pregunta Canónica", out_preg)
    generar_pareto(df_resp, "Respuesta Canónica", out_resp)
    
    log.info("Fase C completada. Reportes de pareto generados.")
