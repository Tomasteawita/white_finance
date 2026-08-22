"""
analisis_apertura_conversaciones.py
===================================
Analiza el corpus de chats NLP para medir el volumen de palabras por participante 
y determinar quién domina la conversación (Asesor vs Cliente).
"""

import json
import logging
from collections import defaultdict
from pathlib import Path

import pandas as pd

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
log = logging.getLogger(__name__)

INPUT_JSONL = Path("D:/DatosDeMercado/marketing_data/instagram_20260820/processed/chat_corpus_nlp.jsonl")
OUTPUT_CSV = Path("D:/DatosDeMercado/marketing_data/instagram_20260820/processed/analisis_apertura.csv")
TOMAS_NAME_PATTERN = "Cueva"

def contar_palabras(texto: str) -> int:
    if not isinstance(texto, str):
        return 0
    # Remover el tag "[AUDIO TRANSCRITO]:"
    texto_limpio = texto.replace("[AUDIO TRANSCRITO]:", "")
    return len(texto_limpio.split())

def main():
    log.info("Iniciando análisis de apertura en conversaciones...")
    
    if not INPUT_JSONL.exists():
        log.error(f"Archivo no encontrado: {INPUT_JSONL}")
        return

    # Estructura para agrupar por conversación
    conversaciones = defaultdict(lambda: {
        "tomas_words": 0,
        "cliente_words": 0,
        "tomas_msgs": 0,
        "cliente_msgs": 0,
        "cliente_max_msg_len": 0
    })

    with open(INPUT_JSONL, "r", encoding="utf-8") as f:
        for line in f:
            if not line.strip():
                continue
            record = json.loads(line)
            
            conv_id = record["conversation_id"]
            sender = record["sender_name"]
            texto = record["full_text"]
            
            # Ignorar mensajes sin texto
            if not texto or texto.strip() == "":
                continue
                
            word_count = contar_palabras(texto)
            if word_count == 0:
                continue
                
            is_tomas = TOMAS_NAME_PATTERN in sender
            
            if is_tomas:
                conversaciones[conv_id]["tomas_words"] += word_count
                conversaciones[conv_id]["tomas_msgs"] += 1
            else:
                conversaciones[conv_id]["cliente_words"] += word_count
                conversaciones[conv_id]["cliente_msgs"] += 1
                if word_count > conversaciones[conv_id]["cliente_max_msg_len"]:
                    conversaciones[conv_id]["cliente_max_msg_len"] = word_count
                    
    # Filtrar y armar dataframe
    data = []
    for conv_id, stats in conversaciones.items():
        # Solo analizar conversaciones donde AMBOS hablaron
        if stats["tomas_words"] == 0 or stats["cliente_words"] == 0:
            continue
            
        total_words = stats["tomas_words"] + stats["cliente_words"]
        tomas_pct = (stats["tomas_words"] / total_words) * 100
        cliente_pct = (stats["cliente_words"] / total_words) * 100
        
        # Categorizar dominio
        if tomas_pct > 60:
            dominio = "Dominada por Tomas"
        elif cliente_pct > 60:
            dominio = "Dominada por Cliente"
        else:
            dominio = "Equilibrada"
            
        # Categorizar apertura
        apertura = "Baja"
        if stats["cliente_max_msg_len"] > 30:
            apertura = "Alta (Monólogo >30 pals)"
        elif stats["cliente_max_msg_len"] > 10:
            apertura = "Media (>10 pals)"
            
        data.append({
            "conversation_id": conv_id,
            "Total Palabras": total_words,
            "Palabras Tomas": stats["tomas_words"],
            "Palabras Cliente": stats["cliente_words"],
            "% Tomas": round(tomas_pct, 1),
            "% Cliente": round(cliente_pct, 1),
            "Mensajes Tomas": stats["tomas_msgs"],
            "Mensajes Cliente": stats["cliente_msgs"],
            "Max Longitud Msj Cliente": stats["cliente_max_msg_len"],
            "Dominio de Conversación": dominio,
            "Nivel de Apertura": apertura
        })
        
    df = pd.DataFrame(data)
    df.to_csv(OUTPUT_CSV, index=False, encoding="utf-8-sig")
    
    log.info(f"Análisis completado. Total conversaciones con interacción de ambos: {len(df)}")
    
    # Resumen
    resumen_dominio = df["Dominio de Conversación"].value_counts(normalize=True) * 100
    resumen_apertura = df["Nivel de Apertura"].value_counts(normalize=True) * 100
    
    print("\n--- RESUMEN DE DOMINIO DE CONVERSACIÓN ---")
    print(resumen_dominio.round(1))
    
    print("\n--- RESUMEN DE NIVEL DE APERTURA ---")
    print(resumen_apertura.round(1))
    
    # Métricas agregadas
    total_palabras_global = df["Total Palabras"].sum()
    total_tomas_global = df["Palabras Tomas"].sum()
    total_cliente_global = df["Palabras Cliente"].sum()
    
    print("\n--- MÉTRICAS GLOBALES ---")
    print(f"Total de palabras intercambiadas: {total_palabras_global:,}")
    print(f"Total palabras Tomas: {total_tomas_global:,} ({(total_tomas_global/total_palabras_global*100):.1f}%)")
    print(f"Total palabras Cliente: {total_cliente_global:,} ({(total_cliente_global/total_palabras_global*100):.1f}%)")
    
if __name__ == "__main__":
    main()
