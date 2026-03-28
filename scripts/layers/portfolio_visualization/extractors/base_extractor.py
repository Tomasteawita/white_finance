import os
import logging
from abc import ABC, abstractmethod
import pandas as pd
from sqlalchemy import create_engine, text
from dotenv import load_dotenv

logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(name)s - %(levelname)s - %(message)s")

class BaseExtractor(ABC):
    """
    Patrón Template Method: Define el esqueleto del algoritmo de extracción de cotizaciones históricas.
    Garantiza que todas las subclases sigan el pipeline: _authenticate -> _fetch -> _normalize -> _save.
    """
    def __init__(self):
        self.logger = logging.getLogger(self.__class__.__name__)
        self.engine = self._init_db()

    def _init_db(self):
        """Inicializa en el constructor la conexión a Postgres aprovechando SQLAlchemy"""
        load_dotenv(r"c:\Users\tomas\white_finance\.env")
        user = os.getenv("POSTGRE_USER", "postgres")
        pwd = os.getenv("POSTGRE_PASSWORD", "postgres")
        host = os.getenv("POSTGRE_HOST", "localhost")
        port = os.getenv("POSTGRE_PORT", "5432")
        db = os.getenv("POSTGRE_DB", "postgres")
        conn_str = f"postgresql+psycopg2://{user}:{pwd}@{host}:{port}/{db}"
        return create_engine(conn_str)

    def extract(self, ticker: str, start_date: str = None, end_date: str = None, mercado: str = "bCBA") -> bool:
        """
        === THE TEMPLATE METHOD ===
        Pipeline irrestricto de extracción con Asimetría Positiva (Robustez).
        Retorna True si la especie fue asimilada a la Base de Datos exitosamente.

        Args:
            ticker:     Símbolo de la especie (sin sufijos como .BA / .US).
            start_date: Fecha de inicio 'YYYY-MM-DD'. Si None, el extractor usa su default.
            end_date:   Fecha de fin    'YYYY-MM-DD'. Si None, el extractor usa su default.
            mercado:    Mercado de origen. 'bCBA' = BYMA (Argentina), 'nYSE|nASDAQ' = USA.
        """
        # 1. Autenticación Delegada
        if not self._authenticate():
            self.logger.warning(f"Abortado [{ticker}]: Falló la validación pre-vuelo (Auth o Token).")
            return False

        self.logger.info(f"Iniciando flujo de extracción para [{ticker}] | mercado={mercado} ...")

        # 2. Obtención de datos crudos Delegada
        try:
            raw_data = self._fetch_data(ticker, start_date, end_date, mercado=mercado)
        except Exception as e:
            self.logger.error(f"Falla ruidosa al consultar la API original para {ticker}: {e}")
            return False

        if raw_data is None or (isinstance(raw_data, pd.DataFrame) and raw_data.empty):
            self.logger.warning(f"No hay registros crudos en el provider para [{ticker}].")
            return False

        # 3. Normalización Delegada
        try:
            normalized_data = self._normalize_data(raw_data, ticker, mercado=mercado)
        except Exception as e:
            self.logger.error(f"Error normalizando la respuesta de {ticker}: {e}")
            return False

        if normalized_data is None or normalized_data.empty:
             self.logger.warning(f"Dataframes resultaron vacios tras la normalización para [{ticker}].")
             return False

        # 4. Inserción Concreta (Implementado aquí para Re-usabilidad)
        try:
            self._save_to_db(ticker, normalized_data)
            self.logger.info(f"✅ Extracción y Save concluído: {len(normalized_data)} filas de [{ticker}].")
            return True
        except Exception as e:
            self.logger.error(f"Error volcando los datos normalizados for {ticker} a DB: {e}")
            return False

    @abstractmethod
    def _authenticate(self) -> bool:
        """Evalúa si la herramienta tiene autorizacion. Las libres pueden retornar True fijo."""
        pass

    @abstractmethod
    def _fetch_data(self, ticker: str, start_date: str = None, end_date: str = None, mercado: str = "bCBA") -> pd.DataFrame:
        """Peticiona a la red de la fuente externa particular y entrega los datos sucios resultantes."""
        pass

    @abstractmethod
    def _normalize_data(self, raw_data: pd.DataFrame, ticker: str, mercado: str = "bCBA") -> pd.DataFrame:
        """
        Debe tomar origin data, homogeneizar columnas y retornar estricto:
        date (DATE/Y-M-D), open, high, low, close, volume, source (str)
        """
        pass

    def _save_to_db(self, ticker: str, df: pd.DataFrame):
        """Idempotencia: Sobrescribe records del provider para evitar colisiones PK (ticker, date)."""
        df_db = df.copy()
        
        # Validation
        if 'ticker' not in df_db.columns:
            df_db['ticker'] = ticker
            
        source = df_db['source'].iloc[0] if 'source' in df_db.columns else self.__class__.__name__
        df_db['source'] = source
        
        expected_cols = ['ticker', 'date', 'open', 'high', 'low', 'close', 'volume', 'source']
        for col in expected_cols:
            if col not in df_db.columns:
                df_db[col] = None
        df_db = df_db[expected_cols]

        # Borrado pre-inserción vs source & ticker para evitar Primary Key constraint violation
        with self.engine.connect() as conn:
            with conn.begin():
                conn.execute(
                    text("DELETE FROM earnings.historical_prices WHERE ticker = :t AND (source = :s OR source = :s2)"), 
                    {"t": ticker, "s": source, "s2": self.__class__.__name__}
                )
        df_db.to_sql('historical_prices', self.engine, schema='earnings', if_exists='append', index=False)
