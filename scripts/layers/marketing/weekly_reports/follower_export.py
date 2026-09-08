import json
import csv
import logging
from datetime import datetime
from pathlib import Path

log = logging.getLogger(__name__)

def export_followers_href_to_csv(followers_file: Path, start_date_str: str, end_date_str: str, output_csv_path: Path):
    """
    Lee el archivo JSON de seguidores y exporta un CSV con el 'href' de los seguidores
    que comenzaron a seguir en el rango de fechas [start_date_str, end_date_str].
    """
    if not followers_file.exists():
        log.warning(f"follower_export: No se encontró el archivo de seguidores: {followers_file}")
        return
        
    start_date = datetime.strptime(start_date_str, "%Y-%m-%d").date()
    end_date = datetime.strptime(end_date_str, "%Y-%m-%d").date()
    
    try:
        with open(followers_file, 'r', encoding='utf-8') as f:
            data = json.load(f)
    except Exception as e:
        log.error(f"follower_export: Error leyendo {followers_file}: {e}")
        return
        
    lista_seguidores = []
    if isinstance(data, list):
        lista_seguidores = data
    elif isinstance(data, dict):
        for k, v in data.items():
            if isinstance(v, list):
                lista_seguidores = v
                break
                
    hrefs_to_export = []
    
    for item in lista_seguidores:
        if "string_list_data" in item and len(item["string_list_data"]) > 0:
            follower_data = item["string_list_data"][0]
            timestamp = follower_data.get("timestamp", 0)
            href = follower_data.get("href", "")
            
            if timestamp and href:
                if timestamp > 1e11: 
                    timestamp = timestamp / 1000.0
                try:
                    msg_date = datetime.fromtimestamp(timestamp).date()
                    if start_date <= msg_date <= end_date:
                        hrefs_to_export.append([href])
                except Exception as e:
                    pass
                    
    try:
        with open(output_csv_path, 'w', encoding='utf-8-sig', newline='') as csvfile:
            writer = csv.writer(csvfile)
            writer.writerow(['href'])
            writer.writerows(hrefs_to_export)
        log.info(f"follower_export: Exportados {len(hrefs_to_export)} enlaces de seguidores a {output_csv_path}")
    except Exception as e:
        log.error(f"follower_export: Error guardando CSV {output_csv_path}: {e}")
