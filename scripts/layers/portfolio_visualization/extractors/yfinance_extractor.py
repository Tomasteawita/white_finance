import pandas as pd
import yfinance as yf
from extractors.base_extractor import BaseExtractor

class YFinanceExtractor(BaseExtractor):
    """
    Subclase concreta de BaseExtractor para el proveedor Yahoo Finance.
    """
    def _authenticate(self) -> bool:
        # YFinance doesn't need formal authentication
        return True

    def _fetch_data(self, ticker: str, start_date: str = None, end_date: str = None) -> pd.DataFrame:
        # Determine if it needs '.BA'. For now, naive approach: 
        # If it has .US it is global. If it has no suffix and it's 3-4 chars, assume .BA? 
        # The pipeline will try the exact ticker given, or we can append .BA if it's local.
        # Since the orchestrator receives the raw ticker string from the CSV, 
        # let's try exactly what is requested.
        
        self.logger.info(f"[YFinance] Consultando origen para: {ticker}")
        ticker_obj = yf.Ticker(ticker)
        # Using 5y as standard history depth for the quantitative requirement
        # If start_date explicitly given, can override.
        hist = ticker_obj.history(period="5y")
        return hist

    def _normalize_data(self, raw_data: pd.DataFrame, ticker: str) -> pd.DataFrame:
        df = raw_data.reset_index()
        
        # Normalize Index
        if 'Date' in df.columns:
            if df['Date'].dt.tz is not None:
                df['date'] = df['Date'].dt.tz_localize(None).dt.date
            else:
                df['date'] = df['Date'].dt.date
        else:
            self.logger.warning(f"Malformación de la columna Fecha al normalizar {ticker}")
            return pd.DataFrame()
            
        df['source'] = 'YFinance'
        df.rename(columns={'Open': 'open', 'High': 'high', 'Low': 'low', 'Close': 'close', 'Volume': 'volume'}, inplace=True)
        return df[['date', 'open', 'high', 'low', 'close', 'volume', 'source']]
