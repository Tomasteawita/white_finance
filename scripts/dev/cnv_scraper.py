import os
import re
import time
from datetime import datetime
from playwright.sync_api import sync_playwright

def parse_spanish_date(date_str):
    """
    Parsea fechas en formato '18 sep. 2026' a objetos datetime.
    """
    date_str = re.sub(r'\s+', ' ', date_str).strip()
    meses = {
        'ene.': '01', 'ene': '01',
        'feb.': '02', 'feb': '02',
        'mar.': '03', 'mar': '03',
        'abr.': '04', 'abr': '04',
        'may.': '05', 'may': '05',
        'jun.': '06', 'jun': '06',
        'jul.': '07', 'jul': '07',
        'ago.': '08', 'ago': '08',
        'sep.': '09', 'sep': '09',
        'oct.': '10', 'oct': '10',
        'nov.': '11', 'nov': '11',
        'dic.': '12', 'dic': '12'
    }
    try:
        parts = date_str.split(' ')
        day = int(parts[0])
        month_str = parts[1].lower()
        year = int(parts[2])
        month = int(meses.get(month_str, '01'))
        return datetime(year, month, day)
    except Exception as e:
        print(f"Error parseando la fecha '{date_str}': {e}")
        return None

def run():
    start_date = datetime(2026, 1, 1)
    end_date = datetime(2026, 5, 5)
    
    # Archivo HTML local donde buscaremos los enlaces
    html_file_path = r"c:\Users\User\white_finance\data\analytics\cotizaciones\hechos_relevants.html"
    
    download_dir = os.path.abspath(os.path.join(os.path.dirname(__file__), '..', '..', 'data', 'cnv_downloads'))
    os.makedirs(download_dir, exist_ok=True)
    print(f"Los archivos se guardarán en: {download_dir}")
    
    # Sistema de reanudación
    historial_path = os.path.join(download_dir, "historial_descargas.log")
    descargados = set()
    if os.path.exists(historial_path):
        with open(historial_path, "r", encoding="utf-8") as f:
            for line in f:
                descargados.add(line.strip())
    
    with sync_playwright() as p:
        browser = p.chromium.launch(headless=False)
        context = browser.new_context(accept_downloads=True)
        page = context.new_page()
        
        print(f"Cargando el archivo local HTML...")
        file_uri = "file:///" + html_file_path.replace("\\", "/")
        page.goto(file_uri)
        
        print("Analizando los registros de la tabla local...")
        rows = page.locator("table.tabla-hechos-relevantes tbody tr").element_handles()
        links_to_download = []
        
        for row in rows:
            date_col = row.query_selector("td:first-child a")
            if date_col:
                date_text = date_col.inner_text()
                link = date_col.get_attribute("href")
                
                row_date = parse_spanish_date(date_text)
                if row_date and start_date <= row_date <= end_date:
                    if link:
                        if link.startswith('/'):
                            link = "https://aif2.cnv.gov.ar" + link
                        elif not link.startswith('http'):
                            link = "https://aif2.cnv.gov.ar/Presentations/publicview/" + link
                            
                        links_to_download.append((row_date, link))
                        
        print(f"Encontrados {len(links_to_download)} documentos para descargar en el rango solicitado.")
        
        batch_count = 0
        
        # Iteramos secuencialmente
        for row_date, link in links_to_download:
            if link in descargados:
                print(f"Saltando (ya existe en historial): {link}")
                continue

            try:
                print(f"\nAbriendo {link} (Fecha: {row_date.strftime('%Y-%m-%d')})")
                new_page = context.new_page()
                new_page.goto(link, timeout=60000)
                
                print("Iniciando descarga...")
                with new_page.expect_download(timeout=45000) as download_info:
                    new_page.locator("i.icon-cloud-download").click()
                
                download = download_info.value
                filename = download.suggested_filename
                filepath = os.path.join(download_dir, filename)
                download.save_as(filepath)
                print(f"✓ Descargado correctamente: {filename}")
                new_page.close()

                # Guardamos en el historial para reanudación
                with open(historial_path, "a", encoding="utf-8") as f:
                    f.write(link + "\n")
                    descargados.add(link)
                
                # Aumentamos el contador de descargas de este lote
                batch_count += 1
                
                # Chequeamos si llegamos a 4
                if batch_count >= 4:
                    print("\n--- Lote de 4 alcanzado ---")
                    print("Esperando 4 minutos (240 segundos) antes de reanudar las descargas...")
                    time.sleep(240)
                    batch_count = 0
                else:
                    # Pausa pequeña de 2 segundos para asegurar estabilidad entre descargas consecutivas
                    time.sleep(2)

            except Exception as e:
                print(f"x Error descargando desde {link}: {e}")
                try:
                    if not new_page.is_closed():
                        new_page.close()
                except:
                    pass
                
        browser.close()
        print("\nProceso completado.")

if __name__ == "__main__":
    run()
