import logging
import os
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
            PrimaryExtractor(),
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

    def run(self, tickers: list, start_date: str = None, end_date: str = None):
        """
        Ejecuta el pipeline.
        Toma una lista de especies deseadas, verifica si faltan en DB y hace fallback.
        """
        self.logger.info("=== INICIANDO PIPELINE DE EXTRACCION ===")
        mis_tickers = self.get_missing_tickers(tickers)
        
        if not mis_tickers:
            self.logger.info("Todas las especies solicitadas ya se encuentran consolidadas en base de datos.")
            return
            
        self.logger.info(f"Especies faltantes a orquestar ({len(mis_tickers)}): {mis_tickers}")
        
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

    def _get_tickers_from_s3(self) -> list:
        import boto3
        import io
        import pandas as pd
        
        s3 = boto3.client('s3')
        bucket = "withefinance-integrated" # As specified in S3 ARN
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
    # Para testing o disparador automático, llamamos directo a run_s3_pipeline
    pipeline.run_s3_pipeline()
