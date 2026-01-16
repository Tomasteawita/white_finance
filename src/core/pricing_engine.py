"""
Motor de precios - Descarga y gestión de datos de mercado
"""

import pandas as pd
import yfinance as yf
from pathlib import Path
from typing import List, Optional
from datetime import datetime, timedelta
import yaml


class PricingEngine:
    """
    Gestiona la descarga y actualización de precios históricos
    Mantiene un archivo maestro con precios incrementales
    """
    
    def __init__(self, processed_data_path: Path, config_path: Path):
        self.processed_data_path = processed_data_path
        self.prices_path = processed_data_path / "prices"
        self.prices_path.mkdir(parents=True, exist_ok=True)
        
        self.master_prices_file = self.prices_path / "master_prices.csv"
        self.assets_mapping = self._load_assets_mapping(config_path)
    
    def _load_assets_mapping(self, config_path: Path) -> dict:
        """Carga el mapeo de activos desde config"""
        mapping_file = config_path / "assets_mapping.yaml"
        if mapping_file.exists():
            with open(mapping_file, 'r', encoding='utf-8') as f:
                return yaml.safe_load(f)
        return {}
    
    def get_master_prices(self) -> Optional[pd.DataFrame]:
        """Carga el archivo maestro de precios"""
        if self.master_prices_file.exists():
            df = pd.read_csv(self.master_prices_file, parse_dates=['date'])
            return df
        return None
    
    def get_ticker_yahoo(self, ticker: str) -> str:
        """Convierte ticker del broker a formato Yahoo Finance"""
        return self.assets_mapping.get(ticker, ticker)
    
    def download_prices(self, tickers: List[str], start_date: Optional[str] = None, 
                       end_date: Optional[str] = None) -> pd.DataFrame:
        """
        Descarga precios históricos de Yahoo Finance
        """
        if start_date is None:
            start_date = (datetime.now() - timedelta(days=365)).strftime('%Y-%m-%d')
        if end_date is None:
            end_date = datetime.now().strftime('%Y-%m-%d')
        
        # Convertir tickers a formato Yahoo
        yahoo_tickers = [self.get_ticker_yahoo(t) for t in tickers]
        
        all_prices = []
        
        for i, ticker in enumerate(tickers):
            yahoo_ticker = yahoo_tickers[i]
            print(f"Descargando {ticker} ({yahoo_ticker})...")
            
            try:
                data = yf.download(yahoo_ticker, start=start_date, end=end_date, 
                                 progress=False, auto_adjust=True)
                
                if not data.empty:
                    df = data.reset_index()
                    df['ticker'] = ticker
                    df['yahoo_ticker'] = yahoo_ticker
                    df = df.rename(columns={
                        'Date': 'date',
                        'Open': 'open',
                        'High': 'high',
                        'Low': 'low',
                        'Close': 'close',
                        'Volume': 'volume'
                    })
                    all_prices.append(df)
            except Exception as e:
                print(f"  ✗ Error descargando {ticker}: {e}")
        
        if all_prices:
            return pd.concat(all_prices, ignore_index=True)
        return pd.DataFrame()
    
    def update_master_prices(self, new_prices: pd.DataFrame):
        """
        Actualiza el archivo maestro con nuevos precios (upsert)
        """
        master = self.get_master_prices()
        
        if master is None or master.empty:
            merged = new_prices.copy()
        else:
            # Combinar y eliminar duplicados
            merged = pd.concat([master, new_prices], ignore_index=True)
            merged = merged.drop_duplicates(subset=['date', 'ticker'], keep='last')
        
        # Ordenar por ticker y fecha
        merged = merged.sort_values(['ticker', 'date']).reset_index(drop=True)
        
        # Guardar
        merged.to_csv(self.master_prices_file, index=False)
        print(f"✓ Precios actualizados: {len(merged)} registros en {self.master_prices_file}")
        
        return merged
    
    def get_latest_price(self, ticker: str) -> Optional[float]:
        """Obtiene el último precio disponible de un ticker"""
        master = self.get_master_prices()
        if master is None:
            return None
        
        ticker_data = master[master['ticker'] == ticker]
        if ticker_data.empty:
            return None
        
        return ticker_data.sort_values('date').iloc[-1]['close']
    
    def get_price_at_date(self, ticker: str, date: datetime) -> Optional[float]:
        """Obtiene el precio de un ticker en una fecha específica"""
        master = self.get_master_prices()
        if master is None:
            return None
        
        ticker_data = master[
            (master['ticker'] == ticker) & 
            (master['date'] == pd.to_datetime(date).normalize())
        ]
        
        if ticker_data.empty:
            return None
        
        return ticker_data.iloc[0]['close']
