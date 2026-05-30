"""
La información sale obtenida de acá:
https://www.cnv.gov.ar/SitioWeb/FondosComunesInversion/CuotaPartes
"""

import os
import sys
import glob
import json
import logging
import pandas as pd
from datetime import datetime

class FCICNVExtractor:
    """
    Clase encargada de extraer las cotizaciones de los FCIs desde los
    archivos de Excel publicados por la CNV, filtrando según los mapas
    definidos para los brokers.
    """
    def __init__(self):
        # Rutas dinámicas
        self.base_path = os.path.normpath(os.path.join(os.path.dirname(os.path.abspath(__file__)), '../../../'))
        self.cnv_files_pattern = os.path.join(
            self.base_path,
            'data/analytics/cotizaciones/FCIs_cnv.gov.ar_SitioWeb_FondosComunesInversion_Cuotapartes',
            '*.xlsx'
        )
        self.map_fci_path = os.path.join(self.base_path, 'data/balanz/maps_fci.json')
        self.output_csv = os.path.join(self.base_path, 'data/analytics/cotizaciones/fci_quotes_historico.csv')
        
        # Configuración de Logging
        self.log_dir = os.path.join(self.base_path, 'logs')
        os.makedirs(self.log_dir, exist_ok=True)
        logging.basicConfig(
            filename=os.path.join(self.log_dir, 'balanz_pipelines.log'),
            level=logging.INFO,
            format='%(asctime)s - %(levelname)s - %(name)s - %(message)s',
            datefmt='%Y-%m-%d %H:%M:%S'
        )
        self.logger = logging.getLogger('FCICNVExtractor')
        
    def load_maps(self) -> list:
        """Carga el mapeo de FCIs desde JSON para saber qué fondos buscar, escaneando todos los clientes."""
        all_official_names = set()
        # Buscar en todos los directorios de clientes
        pattern = os.path.join(self.base_path, 'data/balanz/*/maps_fci.json')
        maps_files = glob.glob(pattern)
        
        for map_file in maps_files:
            try:
                with open(map_file, 'r', encoding='utf-8') as f:
                    maps_fci = json.load(f)
                for official_name in maps_fci.values():
                    all_official_names.add(official_name)
            except Exception as e:
                self.logger.error(f"Error cargando {map_file}: {e}")
                
        return list(all_official_names)

    def extract_quotes(self):
        self.logger.info("Iniciando extracción de cotizaciones de FCIs desde reportes de CNV...")
        target_funds = self.load_maps()
        if not target_funds:
            self.logger.warning("No hay fondos para extraer.")
            return

        files = glob.glob(self.cnv_files_pattern)
        if not files:
            self.logger.error(f"CRITICAL: No se encontraron archivos en {self.cnv_files_pattern}")
            return

        all_quotes = []
        for file in files:
            try:
                # Leemos saltando las primeras 5 filas (encabezados irregulares)
                df = pd.read_excel(file, skiprows=5)
                # Renombramos basado en el patrón observado
                df_clean = df.rename(columns={
                    'Unnamed: 0': 'Fondo',
                    'Unnamed: 4': 'Fecha',
                    'Unnamed: 5': 'Valor_mil_cuotapartes'
                })
                # Filtramos filas vacías
                df_clean = df_clean.dropna(subset=['Fondo'])
                
                # Buscamos nuestros fondos
                mask = df_clean['Fondo'].isin(target_funds)
                df_target = df_clean[mask].copy()

                for _, row in df_target.iterrows():
                    # Formatear fecha (la CNV reporta en dd/mm/yy o similar, pandas lo lee como string o datetime)
                    # Forzamos conversión a string para manejo homogéneo
                    fecha_str = str(row['Fecha'])
                    if pd.notna(fecha_str) and fecha_str.strip():
                        # Parse y estandarización a YYYY-MM-DD
                        try:
                            # A veces viene como dd/mm/yy (ej 05/05/26)
                            if '/' in fecha_str:
                                parsed_date = datetime.strptime(fecha_str, '%d/%m/%y')
                                fecha_std = parsed_date.strftime('%Y-%m-%d')
                            else:
                                # Intento de parseo general si es otro formato o datetime object
                                parsed_date = pd.to_datetime(fecha_str)
                                fecha_std = parsed_date.strftime('%Y-%m-%d')
                        except Exception as e:
                            # Si falla, log y se omite
                            self.logger.warning(f"Error parseando fecha {fecha_str} para {row['Fondo']}: {e}")
                            continue

                        # Calcular valor unitario
                        try:
                            valor_unitario = float(row['Valor_mil_cuotapartes']) / 1000.0
                            all_quotes.append({
                                'date': fecha_std,
                                'ticker': row['Fondo'],
                                'close_ars': valor_unitario
                            })
                        except ValueError:
                            self.logger.error(f"Error convirtiendo valor {row['Valor_mil_cuotapartes']} para {row['Fondo']}")
            except Exception as e:
                self.logger.error(f"Error procesando el archivo {file}: {e}")

        if all_quotes:
            df_final = pd.DataFrame(all_quotes)
            # Ordenamos por fondo y luego por fecha
            df_final = df_final.sort_values(by=['ticker', 'date']).drop_duplicates()
            df_final.to_csv(self.output_csv, index=False)
            self.logger.info(f"Extracción exitosa. {len(df_final)} registros guardados en {self.output_csv}")
        else:
            self.logger.warning("No se encontraron registros de cotizaciones para los fondos buscados.")

if __name__ == "__main__":
    extractor = FCICNVExtractor()
    extractor.extract_quotes()
