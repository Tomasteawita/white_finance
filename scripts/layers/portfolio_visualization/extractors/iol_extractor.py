import pandas as pd
from extractors.base_extractor import BaseExtractor
from extractors.iol_manager import IOLManager
import datetime

class IOLExtractor(BaseExtractor):
    """
    Subclase concreta del Template Method apuntando a la InvertirOnline REST API.
    Aprovecha el iol_manager.py nativo para gestionar la autenticación OAuth2 persistente.
    """
    def __init__(self):
        super().__init__()
        self.client = IOLManager()

    def _authenticate(self) -> bool:
        """Invoca el motor validatorio de tokens en el IOLManager"""
        self.logger.info("[IOL] Autorizando identidad OAuth2...")
        try:
            # get_headers() obliga a refrescar/pedir token. 
            headers = self.client.get_headers()
            if headers:
                return True
        except Exception as e:
            self.logger.error(f"[IOL] Bloqueo 401 en autenticación (Requiere habilitación broker): {e}")
        return False

    def _fetch_data(self, ticker: str, start_date: str = None, end_date: str = None) -> pd.DataFrame:
        """Extrae el histórico a través del wrapper get_serie_historica."""
        # Se fijan 5 años hacia atras predeterminado
        fecha_hasta = datetime.datetime.now().strftime("%Y-%m-%d")
        fecha_desde = (datetime.datetime.now() - datetime.timedelta(days=365*5)).strftime("%Y-%m-%d")
        
        # Asume "bCBA" (BYMA) si es un ticker argentino estandar, a menos q sea dolar...
        mercado = "bCBA"
        
        try:
            res = self.client.get_serie_historica(mercado=mercado, simbolo=ticker, fecha_desde=fecha_desde, fecha_hasta=fecha_hasta, ajustada="ajustada")
            if res:
                # El Manager ya retorna listas, lo cargamos en pandas
                return pd.DataFrame(res)
        except Exception as e:
            self.logger.error(f"[IOL] Falla en conexión REST para {ticker}: {e}")
            
        return pd.DataFrame()

    def _normalize_data(self, raw_data: pd.DataFrame, ticker: str) -> pd.DataFrame:
        """
        IOL devuelve algo como:
        "fechaHora": "2024-03-22T00:00:00",
        "apertura": 100,
        "maximo": 110,
        ...
        """
        if raw_data.empty:
            return pd.DataFrame()
            
        df = raw_data.copy()
        
        if 'fechaHora' in df.columns:
            df['date'] = pd.to_datetime(df['fechaHora']).dt.date
        else:
            self.logger.warning(f"[IOL] Estructura JSON imprevista, sin fechaHora: {df.columns}")
            return pd.DataFrame()

        # Rename
        col_map = {
            'apertura': 'open',
            'maximo': 'high',
            'minimo': 'low',
            'ultimoPrecio': 'close',
            'volumen': 'volume'
        }
        df.rename(columns=col_map, inplace=True)
        
        # IOL source tag
        df['source'] = 'IOL_API'
        
        expected = ['date', 'open', 'high', 'low', 'close', 'volume', 'source']
        for c in expected:
            if c not in df.columns:
                df[c] = None
                
        return df[expected]
