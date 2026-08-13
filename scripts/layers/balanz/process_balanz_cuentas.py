import os
import glob
import json
import logging
import pandas as pd
from sqlalchemy import create_engine, text
from dotenv import load_dotenv

# Configuración de Logging
logging.basicConfig(level=logging.INFO, format="%(asctime)s - [%(levelname)s] - %(message)s")
logger = logging.getLogger("BalanzProcessor")

class BalanzAccountProcessor:
    def __init__(self):
        self.base_path = r"c:\Users\tomas\white_finance"
        self.data_balanz_path = os.path.join(self.base_path, "data", "balanz")
        self.cnv_quotes_path = os.path.join(self.base_path, "data", "analytics", "cotizaciones", "FCIs_cnv.gov.ar_SitioWeb_FondosComunesInversion_Cuotapartes")
        self.engine = self._init_db()

    def _init_db(self):
        load_dotenv(os.path.join(self.base_path, ".env"))
        user = os.getenv("POSTGRE_USER", "postgres")
        pwd = os.getenv("POSTGRE_PASSWORD", "postgres")
        host = os.getenv("POSTGRE_HOST", "localhost")
        port = os.getenv("POSTGRE_PORT", "5432")
        db = os.getenv("POSTGRE_DB", "postgres")
        conn_str = f"postgresql+psycopg2://{user}:{pwd}@{host}:{port}/{db}"
        return create_engine(conn_str)

    def process_all_clients(self):
        """Itera por todos los clientes y genera los históricos de cuenta corriente."""
        if not os.path.exists(self.data_balanz_path):
            logger.error(f"El directorio base {self.data_balanz_path} no existe.")
            return

        clients = [d for d in os.listdir(self.data_balanz_path) if os.path.isdir(os.path.join(self.data_balanz_path, d))]
        
        for client in clients:
            logger.info(f"--- Procesando cliente: {client} ---")
            client_dir = os.path.join(self.data_balanz_path, client)
            cc_dir = os.path.join(client_dir, "Cuenta Corriente")
            
            if not os.path.exists(cc_dir):
                logger.warning(f"No existe el directorio 'Cuenta Corriente' para el cliente {client}. Omitiendo.")
                continue
                
            # 1. Unificar y eliminar duplicados
            hist_csv_path = self._unify_account_statements(cc_dir)
            
            if not hist_csv_path:
                continue
                
            # 2. Validar especies faltantes en fuentes internas
            self._validate_missing_quotes(client, client_dir, hist_csv_path)

    def _unify_account_statements(self, cc_dir):
        """Lee todos los excels/csv, los une y quita duplicados (usando 'Numero' si existe)."""
        files = glob.glob(os.path.join(cc_dir, "*.*"))
        valid_files = [f for f in files if f.endswith(".xlsx") or f.endswith(".csv")]
        
        # Ignorar si ya existe el histórico para no leerlo recursivamente como parte de las fuentes
        valid_files = [f for f in valid_files if os.path.basename(f) != "cuenta_corriente_historico.csv"]

        if not valid_files:
            logger.info(f"No se encontraron archivos válidos en {cc_dir}.")
            return None

        dfs = []
        for file in valid_files:
            try:
                if file.endswith(".xlsx"):
                    # Por convención en Balanz, puede que la cabecera no esté en la fila 0, pero asumimos lectura estándar
                    df = pd.read_excel(file)
                else:
                    df = pd.read_csv(file, sep="|")
                dfs.append(df)
            except Exception as e:
                logger.error(f"Error leyendo {file}: {e}")

        if not dfs:
            return None

        df_unified = pd.concat(dfs, ignore_index=True)

        # Regla de Negocio: Usar SIEMPRE el campo `Numero` para deduplicar anomalías de brokers locales
        if 'Numero' in df_unified.columns:
            df_unified = df_unified.drop_duplicates(subset=['Numero'])
        else:
            df_unified = df_unified.drop_duplicates()

        output_path = os.path.join(cc_dir, "cuenta_corriente_historico.csv")
        df_unified.to_csv(output_path, sep="|", index=False)
        logger.info(f"✅ Histórico unificado generado en {output_path} con {len(df_unified)} registros.")
        
        return output_path

    def _validate_missing_quotes(self, client, client_dir, hist_csv_path):
        """
        Lee el CSV histórico y el maps_fci.json, y verifica si las cotizaciones
        faltan en la BD (earnings.historical_prices) o en los excels de la CNV.
        """
        try:
            df_cc = pd.read_csv(hist_csv_path, sep="|")
        except Exception as e:
            logger.error(f"Error leyendo {hist_csv_path}: {e}")
            return

        # Manejo de la columna Descripción según el estándar de Balanz (soporta acentos y problemas de encoding)
        especie_col = 'Especie'
        if 'Especie' not in df_cc.columns:
            try:
                desc_col = [c for c in df_cc.columns if 'Descripc' in c][0]
                df_cc = df_cc.rename(columns={desc_col: 'Descripcion'})
                especie_col = 'Descripcion'
            except IndexError:
                logger.warning(f"No se encontró columna 'Especie' o variante de 'Descripción' en el histórico de {client}. Columnas: {df_cc.columns}")
                return

        especies_presentes = df_cc[especie_col].dropna().astype(str).unique()
        
        # Cargar maps_fci.json si existe
        maps_fci_path = os.path.join(client_dir, "maps_fci.json")
        maps_fci = {}
        if os.path.exists(maps_fci_path):
            with open(maps_fci_path, "r", encoding="utf-8") as f:
                maps_fci = json.load(f)

        # Cargar cotizaciones históricas de FCI extraídas
        fci_quotes_path = os.path.join(self.base_path, "data", "analytics", "cotizaciones", "fci_quotes_historico.csv")
        fci_tickers = set()
        if os.path.exists(fci_quotes_path):
            try:
                df_fci = pd.read_csv(fci_quotes_path)
                if 'ticker' in df_fci.columns:
                    fci_tickers = set(df_fci['ticker'].unique())
            except Exception as e:
                logger.error(f"Error cargando {fci_quotes_path}: {e}")

        missing_species = []
        
        with self.engine.connect() as conn:
            for raw_especie in especies_presentes:
                especie = str(raw_especie).strip()
                extracted_especie = None
                is_fci = False
                
                if especie.startswith("Boleto"):
                    parts = especie.split("/")
                    if len(parts) >= 5:
                        extracted_especie = parts[4].strip()
                elif especie.startswith("Liquidación de Suscripción") or especie.startswith("Liquidación de Rescate") or especie.startswith("Liquidacion de Suscripcion") or especie.startswith("Liquidacion de Rescate"):
                    parts = especie.split("/")
                    if len(parts) >= 3:
                        extracted_especie = parts[2].strip()
                        is_fci = True
                        
                if not extracted_especie:
                    continue
                    
                # Filtrar ruido común
                if extracted_especie.upper() in ["MEP", "PESOS", "DOLARES", "VARIAS", "$"]:
                    continue

                is_missing = True
                
                if is_fci:
                    # 1. Chequear si es un FCI mapeado y buscarlo en fci_quotes_historico.csv
                    mapped_name = None
                    for cc_name, cnv_name in maps_fci.items():
                        if cc_name in extracted_especie:
                            mapped_name = cnv_name
                            break
                            
                    if mapped_name and mapped_name in fci_tickers:
                        is_missing = False
                    elif mapped_name is None:
                        logger.debug(f"FCI '{extracted_especie}' no está mapeado en maps_fci.json de {client}")
                else:
                    # Chequeo en Base de Datos (earnings.historical_prices) para Boletos (Acciones/Cedears/Bonos)
                    search_term_clean = extracted_especie.replace('.BA', '').replace('.US', '')
                    res = conn.execute(
                        text("SELECT 1 FROM earnings.historical_prices WHERE ticker = :t LIMIT 1"),
                        {"t": search_term_clean}
                    ).fetchone()
                    
                    if res:
                        is_missing = False
                
                if is_missing:
                    missing_species.append(extracted_especie)

        if missing_species:
            logger.warning(f"⚠️  Cotizaciones faltantes detectadas para {client}:")
            for m in missing_species:
                logger.warning(f"   - {m}")
        else:
            logger.info(f"✅ Todas las cotizaciones están cubiertas para {client}.")

if __name__ == "__main__":
    processor = BalanzAccountProcessor()
    processor.process_all_clients()
