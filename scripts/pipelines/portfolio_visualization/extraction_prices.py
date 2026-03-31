import logging
import os
import sys
from datetime import date

# Configuración de rutas para importar desde layers
current_dir = os.path.dirname(os.path.abspath(__file__))
layers_path = os.path.abspath(os.path.join(current_dir, "..", "..", "layers", "portfolio_visualization"))
if layers_path not in sys.path:
    sys.path.append(layers_path)

import pandas as pd
from sqlalchemy import create_engine, text
from dotenv import load_dotenv

# Extractores
from extractors.yfinance_extractor import YFinanceExtractor
from extractors.primary_extractor import PrimaryExtractor
from extractors.iol_extractor import IOLExtractor

logging.basicConfig(level=logging.INFO, format="%(asctime)s - [%(levelname)s] - %(message)s")

class ExtractionPipeline:
    """
    Orquestador del proceso de extracción.
    Verifica especies faltantes en la DB y delega la responsabilidad en cascada:
    YFinance -> Primary (Matriz) -> IOL
    """
    def __init__(self):
        self.logger = logging.getLogger("ExtractionPipeline")
        self.engine = self._init_db()
        
        # Instanciar los workers según la prioridad del usuario
        self.extractors = [
            YFinanceExtractor(),
            # PrimaryExtractor(),
            IOLExtractor()
        ]

    def _init_db(self):
        load_dotenv(r"c:\Users\tomas\white_finance\.env")
        user = os.getenv("POSTGRE_USER", "postgres")
        pwd = os.getenv("POSTGRE_PASSWORD", "postgres")
        host = os.getenv("POSTGRE_HOST", "localhost")
        port = os.getenv("POSTGRE_PORT", "5432")
        db = os.getenv("POSTGRE_DB", "postgres")
        
        conn_str = f"postgresql+psycopg2://{user}:{pwd}@{host}:{port}/{db}"
        return create_engine(conn_str)
        
    def get_missing_tickers(self, required_tickers: list) -> list:
        """Filtra la lista descartando los que ya tienen presencia en la tabla historical_prices."""
        missing = []
        try:
            with self.engine.connect() as conn:
                for req in required_tickers:
                    # Limpiamos el .BA o .US si llega sucio
                    clean_req = req.replace('.BA', '').replace('.US', '')
                    res = conn.execute(
                        text("SELECT 1 FROM earnings.historical_prices WHERE ticker = :t LIMIT 1"),
                        {"t": clean_req}
                    ).fetchone()
                    if not res:
                        missing.append(req)
        except Exception as e:
            self.logger.error(f"Falla consultando especies existentes DB: {e}")
            return required_tickers # En caso de falla, pedimos todos (idempotencia lo limpiará)
            
        return missing

    def run(self, tickers: list, start_date: str = "2023-01-01", end_date: str = None):
        """
        Ejecuta el pipeline.
        Toma una lista de especies deseadas y fuerza la subida completa desde start_date.
        """
        self.logger.info("=== INICIANDO PIPELINE DE EXTRACCION (FORZADO HISTORICO COMPLETO) ===")
        mis_tickers = tickers # Ya no filtramos por existentes para asegurar el refresh completo
        
        if not mis_tickers:
            self.logger.info("No se proporcionaron especies para procesar.")
            return
            
        self.logger.info(f"Especies a orquestar ({len(mis_tickers)}): {mis_tickers}")
        
        for t in mis_tickers:
            self.logger.info(f"\n--- Procesando [{t}] ---")
            success = False
            
            for extractor in self.extractors:
                extractor_name = extractor.__class__.__name__
                self.logger.info(f"--> Intentando con {extractor_name}")
                
                search_ticker = t
                if extractor_name == "YFinanceExtractor" and not t.endswith('.BA') and not t.endswith('.US'):
                    search_ticker = t

                if extractor.extract(search_ticker, start_date, end_date):
                    self.logger.info(f"✅ [{t}] Resultó exitoso a través de {extractor_name}. Cortocircuito activo.")
                    success = True
                    break
                else:
                    self.logger.warning(f"❌ [{t}] Falló con {extractor_name}. Derivando al siguiente fallback...")
            
            if not success:
                self.logger.error(f"⚠️ AGOTADOS TODOS LOS FALLBACKS PARA [{t}]. Ningún origen devolvió información útil.")

        self.logger.info("=== PIPELINE FINALIZADO ===")

    # ------------------------------------------------------------------
    # Pipeline de cotizaciones faltantes basado en cuentas corrientes
    # ------------------------------------------------------------------
    def run_from_cuentas_corrientes(
        self,
        csv_path: str = r"c:\Users\tomas\white_finance\data\analytics\cuentas_unificadas_sorted.csv",
    ) -> None:
        """
        Orquestador que detecta las especies faltantes o desactualizadas en
        `earnings.historical_prices` a partir del CSV consolidado de cuentas
        corrientes y dispara la extraccion de precios historicos.

        Pasos:
        1.  Lee el CSV -> columna 'Especie'.
        2.  Genera columna 'mercado': '.US' -> 'nYSE|nASDAQ', resto -> 'bCBA'.
            Elimina sufijo '.US' y descarta MEP / VARIAS.
        3.  Consulta earnings.ratios_cedears (ultima partition_date).
        4.  Left join Especie <- ticker de ratios_cedears; si match -> mercado='nYSE|nASDAQ'.
        5.  Deduplica y elimina columnas auxiliares (ticker, ratio).
        6.  Left join con max_date de earnings.historical_prices.
        7.  Por cada especie: sin max_date -> desde 2020-01-01;
            con max_date -> desde max_date + 1 dia. Extrae hasta hoy.
        8.  Delega en cadena YFinance -> IOL pasando el 'mercado' a cada extractor.
        """
        self.logger.info("=== INICIANDO PIPELINE DESDE CUENTAS CORRIENTES ===")

        # Paso 1: Leer CSV
        try:
            df = pd.read_csv(csv_path, usecols=["Especie"])
            self.logger.info(f"CSV leido: {len(df)} filas en bruto.")
        except Exception as exc:
            self.logger.error(f"No se pudo leer el CSV '{csv_path}': {exc}")
            return

        df["Especie"] = df["Especie"].astype(str).str.strip()
        df = df.dropna(subset=["Especie"])  # eliminar NaN antes del apply
        df = df[df["Especie"].str.upper() != "NAN"]  # casteo de NaN a string

        # Paso 2: Columna mercado + limpiar sufijo + filtrar
        df["mercado"] = df["Especie"].str.endswith(".US").map(
            {True: "nYSE|nASDAQ", False: "bCBA"}
        )
        df["Especie"] = df["Especie"].str.replace(r"\.US$", "", regex=True)
        df = df[~df["Especie"].str.upper().isin({"MEP", "VARIAS"})].dropna(subset=["Especie"])

        # Paso 3: Leer ratios_cedears (ultima particion)
        query_ratios = """
            SELECT ticker, ratio
            FROM earnings.ratios_cedears
            WHERE partition_date = (SELECT MAX(partition_date) FROM earnings.ratios_cedears);
        """
        try:
            df_ratios: pd.DataFrame = pd.read_sql(text(query_ratios), con=self.engine)
            self.logger.info(f"Tickers en ratios_cedears: {len(df_ratios)}")
        except Exception as exc:
            self.logger.error(f"Error consultando earnings.ratios_cedears: {exc}")
            return

        # Paso 4: Left join con ratios_cedears -> si match, mercado = 'nYSE|nASDAQ'
        df = df.merge(df_ratios[["ticker", "ratio"]], how="left", left_on="Especie", right_on="ticker")
        df.loc[df["ticker"].notna(), "mercado"] = "nYSE|nASDAQ"

        # Paso 5: Deduplicar y eliminar columnas auxiliares
        df = (
            df.drop_duplicates(subset=["Especie"])
            .drop(columns=["ticker", "ratio"], errors="ignore")
            .reset_index(drop=True)
        )
        self.logger.info(f"Especies unicas a evaluar: {len(df)}")

        # Paso 6: Left join con max_date de historical_prices
        query_hist = """
            SELECT ticker, MAX(date) AS max_date
            FROM earnings.historical_prices
            GROUP BY ticker;
        """
        try:
            df_hist: pd.DataFrame = pd.read_sql(text(query_hist), con=self.engine)
            self.logger.info(f"Tickers en historical_prices: {len(df_hist)}")
        except Exception as exc:
            self.logger.error(f"Error consultando earnings.historical_prices: {exc}")
            return

        df_joined: pd.DataFrame = df.merge(
            df_hist, how="left", left_on="Especie", right_on="ticker"
        )

        today_str: str = date.today().isoformat()
        start_date = "2023-01-01"

        # Pasos 7 y 8: Iterar y delegar en extractores con mercado (siempre desde 2023-01-01)
        for _, row in df_joined.iterrows():
            especie: str = row["Especie"]
            mercado_especie: str = row["mercado"]

            self.logger.info(
                f"[{especie}] Forzando subida completa desde {start_date} hasta hoy | mercado={mercado_especie}"
            )

            success = False
            for extractor in self.extractors:
                extractor_name = extractor.__class__.__name__
                if extractor.extract(especie, start_date, today_str, mercado=mercado_especie):
                    self.logger.info(f"[{especie}] Extraido via {extractor_name}.")
                    success = True
                    break
                else:
                    self.logger.warning(
                        f"[{especie}] Fallo con {extractor_name}. Probando siguiente fallback..."
                    )

            if not success:
                self.logger.error(
                    f"[{especie}] Agotados todos los fallbacks. Sin datos disponibles."
                )

        self.logger.info("=== PIPELINE CUENTAS CORRIENTES FINALIZADO ===")

    def _get_tickers_from_s3(self) -> list:
        import boto3
        import io
        import pandas as pd
        
        s3 = boto3.client('s3')
        bucket = "withefinance-integrated"
        prefix = "cuenta_corriente_historico/"
        
        tickers = set()
        self.logger.info(f"Escaneando S3: s3://{bucket}/{prefix}")
        
        try:
            response = s3.list_objects_v2(Bucket=bucket, Prefix=prefix)
            if 'Contents' not in response:
                self.logger.warning(f"No se encontraron archivos en s3://{bucket}/{prefix}")
                return []
                
            for obj in response['Contents']:
                key = obj['Key']
                if key.endswith('.csv'):
                    self.logger.info(f"Procesando archivo remoto: {key}")
                    csv_obj = s3.get_object(Bucket=bucket, Key=key)
                    df = pd.read_csv(io.BytesIO(csv_obj['Body'].read()))
                    
                    if 'Especie' in df.columns:
                        specs = df['Especie'].dropna().astype(str).str.strip().unique()
                        for esp in specs:
                            if esp and esp.upper() not in {'MEP', 'VARIAS'}:
                                tickers.add(esp)
        except Exception as e:
            self.logger.error(f"Falla crítica leyendo S3 ({bucket}): {e}")
            
        return list(tickers)

    def run_s3_pipeline(self, start_date: str = None, end_date: str = None):
        """
        Paso maestro de automatización:
        Lee cuentas de S3 -> Filtra ausentes en BD -> Ejecuta Cadena de Extracción.
        """
        self.logger.info("=== INICIANDO PIPELINE AUTOMATIZADO DESDE S3 ===")
        tickers_s3 = self._get_tickers_from_s3()
        self.logger.info(f"Total especies extraídas de los CSVs en S3: {len(tickers_s3)}")
        
        if tickers_s3:
            self.run(tickers_s3, start_date, end_date)

if __name__ == "__main__":
    pipeline = ExtractionPipeline()
    # Pipeline principal: cotizaciones faltantes / desactualizadas desde cuentas corrientes
    pipeline.run_from_cuentas_corrientes()
