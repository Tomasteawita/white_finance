"""
generar_preguntas_abiertas.py
=============================
Lee los dolores consolidados extraídos de las conversaciones y utiliza Gemini
para generar preguntas abiertas estratégicas (estilo setting) que permitan
al asesor hablar menos y que el prospecto hable más sobre su dolor y futuro deseado.
"""

import os
import json
import logging
from pathlib import Path

import pandas as pd
from google import genai
from google.genai import types
from dotenv import load_dotenv

# Configuración
load_dotenv()
logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
log = logging.getLogger(__name__)

INPUT_CSV = Path("D:/DatosDeMercado/marketing_data/instagram_20260820/processed/dolores_todas_consolidados.csv")
OUTPUT_CSV = Path("D:/DatosDeMercado/marketing_data/instagram_20260820/processed/preguntas_abiertas_setting_todas.csv")

GEMINI_API_KEY = os.getenv("GEMINI_API_KEY", "")
GEMINI_MODEL = "gemini-flash-latest"

def generar_preguntas_para_dolor(dolor: str, client: genai.Client) -> list:
    prompt = (
        "Eres un experto en ventas, 'setting' y psicología del consumidor para servicios financieros en Argentina.\n"
        f"Un prospecto en Instagram DMs expresó el siguiente dolor principal: '{dolor}'.\n\n"
        "Tu objetivo es darme 3 preguntas abiertas estratégicas para responderle o continuar la conversación.\n"
        "Estas preguntas deben:\n"
        "1. Conectar empáticamente con su dolor o problema.\n"
        "2. Indagar sobre su 'futuro deseado' (qué quiere lograr al resolverlo).\n"
        "3. Obligar a la persona a explayarse y hablar más (preguntas abiertas, nunca de sí/no).\n"
        "4. Estar escritas en español rioplatense/argentino natural y conversacional.\n\n"
        "Responde UNICAMENTE con un JSON válido con el siguiente formato exacto:\n"
        '{"preguntas": ["pregunta 1", "pregunta 2", "pregunta 3"]}'
    )
    
    try:
        response = client.models.generate_content(
            model=GEMINI_MODEL,
            contents=prompt,
            config=types.GenerateContentConfig(
                temperature=0.7,
                max_output_tokens=1024,
            )
        )
        texto = response.text.strip()
        
        # Buscar bloque JSON
        import re
        json_match = re.search(r'\{.*\}', texto, re.DOTALL)
        if json_match:
            datos = json.loads(json_match.group())
            return datos.get("preguntas", [])
        else:
            log.warning(f"No se encontró JSON para el dolor: {dolor}")
            return []
    except Exception as e:
        log.error(f"Error procesando el dolor '{dolor}': {e}")
        return []

def main():
    log.info("Iniciando generación de preguntas abiertas...")
    if not GEMINI_API_KEY:
        log.error("GEMINI_API_KEY no encontrada.")
        return
        
    client = genai.Client(api_key=GEMINI_API_KEY)
    
    # Leer dolores
    df = pd.read_csv(INPUT_CSV, encoding="latin-1", on_bad_lines="skip")
    
    # Normalizar nombres de columnas por si hay caracteres raros
    df.columns = ["Dolor", "Porcentaje", "Cantidad"]
    
    # Filtrar dolores inválidos o vacíos
    df_filtrado = df[
        (df["Dolor"].notna()) & 
        (~df["Dolor"].str.contains("Sin dolor expresado", case=False, na=False))
    ].copy()
    
    resultados = []
    
    for _, row in df_filtrado.iterrows():
        dolor = row["Dolor"]
        log.info(f"Generando preguntas para: {dolor}")
        preguntas = generar_preguntas_para_dolor(dolor, client)
        
        for i, p in enumerate(preguntas, 1):
            resultados.append({
                "Dolor del Cliente": dolor,
                "Pregunta Sugerida": p,
                "Orden": i
            })
            
    df_resultados = pd.DataFrame(resultados)
    df_resultados.to_csv(OUTPUT_CSV, index=False, encoding="utf-8-sig")
    log.info(f"Preguntas guardadas exitosamente en: {OUTPUT_CSV}")
    
    # Mostrar resultados
    print("\n--- RESULTADOS ---")
    for dolor, group in df_resultados.groupby("Dolor del Cliente"):
        print(f"\nDolor: {dolor}")
        for _, row in group.iterrows():
            print(f"  - {row['Pregunta Sugerida']}")

if __name__ == "__main__":
    main()
