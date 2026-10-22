import json
import logging
import re
from pathlib import Path
import pandas as pd
from datetime import datetime

log = logging.getLogger(__name__)

TOMAS_NAME_PATTERN = "Cueva"
KEYWORDS = ["DATOS", "PLANIFICACION", "FINANZAS", "DECISIONES", "FLUJO", "ANALISIS"]
RE_EMAIL = re.compile(r"[a-zA-Z0-9_.+-]+@[a-zA-Z0-9-]+\.[a-zA-Z0-9-.]+")
RE_CALENDLY = re.compile(r"calendly\.com", re.IGNORECASE)
RE_PHONE = re.compile(r"(?:\+?\d{1,3}[\s-]?)?\(?\d{2,4}\)?[\s-]?\d{3,4}[\s-]?\d{3,4}")

def has_phone_context(text: str) -> bool:
    text_lower = text.lower()
    keywords = ["wa.me", "whatsapp", "wpp", "celu", "numero", "número", "contacto", "te paso", "mi tel"]
    if any(kw in text_lower for kw in keywords):
        return True
    if "+" in text:
        return True
    return False

def detectar_conversion(mensajes_prospecto: list[str], mensajes_tomas: list[str]) -> dict:
    conversion = {
        "is_converted": False,
        "has_email": False,
        "has_phone": False,
        "has_calendly": False
    }
    
    for msg in mensajes_tomas + mensajes_prospecto:
        if RE_CALENDLY.search(msg):
            conversion["has_calendly"] = True
            conversion["is_converted"] = True
            break
            
    for msg in mensajes_prospecto:
        if not conversion["has_email"] and RE_EMAIL.search(msg):
            conversion["has_email"] = True
            conversion["is_converted"] = True
            
        if not conversion["has_phone"]:
            phone_matches = RE_PHONE.findall(msg)
            for match in phone_matches:
                digits = re.sub(r"\D", "", match)
                if 8 <= len(digits) <= 15:
                    if has_phone_context(msg) or len(digits) == 10 or len(digits) == 12:
                        conversion["has_phone"] = True
                        conversion["is_converted"] = True
                        break
                        
        if conversion["has_email"] and conversion["has_phone"] and conversion["has_calendly"]:
            break
            
    return conversion

def run(process_date: str, start_date: str, end_date: str):
    log.info(f"Fase A: Iniciando medición (process={process_date}, start={start_date}, end={end_date})")
    
    input_jsonl = Path(f"D:/DatosDeMercado/marketing_data/instagram_{process_date}/processed/chat_corpus_nlp.jsonl")
    reports_dir = Path(r"C:\Users\User\white_finance\reports\marketing")
    output_csv = reports_dir / "conversiones_embudo.csv"
    output_ids = reports_dir / "conversaciones_exitosas_ids.json"
    
    if not input_jsonl.exists():
        log.error(f"Archivo no encontrado: {input_jsonl}")
        return

    try:
        dt_start = datetime.strptime(start_date, "%Y-%m-%d")
        dt_end = datetime.strptime(end_date, "%Y-%m-%d").replace(hour=23, minute=59, second=59)
        start_ts = dt_start.timestamp() * 1000
        end_ts = dt_end.timestamp() * 1000
    except ValueError as e:
        log.error(f"Error parseando fechas: {e}")
        return

    conversaciones_validas = {}
    temp_convs = {}
    
    with open(input_jsonl, "r", encoding="utf-8") as f:
        for line in f:
            if not line.strip(): continue
            record = json.loads(line)
            cid = record["conversation_id"]
            if cid not in temp_convs:
                temp_convs[cid] = []
            temp_convs[cid].append(record)
            
    log.info(f"Total de conversaciones cargadas: {len(temp_convs)}")
    
    for cid, msgs in temp_convs.items():
        msgs.sort(key=lambda x: x.get("timestamp_ms", 0))
        
        primer_msg_prospecto = None
        primer_msg_ts = 0
        mensajes_prospecto = []
        mensajes_tomas = []
        
        for msg in msgs:
            es_tomas = TOMAS_NAME_PATTERN in msg.get("sender_name", "")
            texto = msg.get("full_text", "")
            ts = msg.get("timestamp_ms", 0)
            if not texto: continue
            
            if es_tomas:
                mensajes_tomas.append(texto)
            else:
                mensajes_prospecto.append(texto)
                if primer_msg_prospecto is None:
                    primer_msg_prospecto = texto
                    primer_msg_ts = ts
                    
        if not primer_msg_prospecto:
            continue
            
        # Filtrar por ventana de fechas (el primer mensaje debe haber ocurrido en el período)
        if primer_msg_ts < start_ts or primer_msg_ts > end_ts:
            continue
            
        primer_msg_upper = primer_msg_prospecto.strip().upper()
        detected_keyword = None
        for kw in KEYWORDS:
            if kw in primer_msg_upper:
                detected_keyword = kw
                break
                
        if detected_keyword:
            conversaciones_validas[cid] = {
                "keyword": detected_keyword,
                "mensajes_prospecto": mensajes_prospecto,
                "mensajes_tomas": mensajes_tomas
            }

    log.info(f"Conversaciones en fecha con Keywords válidas: {len(conversaciones_validas)}")
    
    resultados = []
    exitosos_ids = []
    resumen_keywords = {kw: {"total": 0, "convertidos": 0} for kw in KEYWORDS}
    
    for cid, data in conversaciones_validas.items():
        kw = data["keyword"]
        resumen_keywords[kw]["total"] += 1
        
        conv_metrics = detectar_conversion(data["mensajes_prospecto"], data["mensajes_tomas"])
        
        if conv_metrics["is_converted"]:
            resumen_keywords[kw]["convertidos"] += 1
            exitosos_ids.append(cid)
            
        resultados.append({
            "conversation_id": cid,
            "keyword_entrada": kw,
            "is_converted": conv_metrics["is_converted"],
            "has_email": conv_metrics["has_email"],
            "has_phone": conv_metrics["has_phone"],
            "has_calendly": conv_metrics["has_calendly"]
        })
        
    with open(output_ids, "w", encoding="utf-8") as f:
        json.dump(exitosos_ids, f, indent=4)
    log.info(f"Se exportaron {len(exitosos_ids)} IDs exitosos a {output_ids}")
    
    df = pd.DataFrame(resultados)
    if not df.empty:
        df.to_csv(output_csv, index=False, encoding="utf-8-sig")
    else:
        log.warning("No hay resultados para guardar en el CSV.")
