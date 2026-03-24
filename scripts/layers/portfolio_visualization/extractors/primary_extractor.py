import pandas as pd
from extractors.base_extractor import BaseExtractor

class PrimaryExtractor(BaseExtractor):
    """
    Subclase concreta de BaseExtractor para el proveedor Primary / Matriz.
    [ESQUELETO PREP-API]
    """
    def _authenticate(self) -> bool:
        """
        En proceso de habilitación institucional.
        Por "Falla Ruidosa" temporal, retorna False si el token de Matriz no es devuelto.
        """
        self.logger.info("[Primary-Matriz] Verificando token de acceso...")
        # Aquí irá pyRofex.initialize(...)
        return False

    def _fetch_data(self, ticker: str, start_date: str = None, end_date: str = None) -> pd.DataFrame:
        """
        Recuperación esqueleto de get_historical_trades()
        """
        self.logger.info(f"[Primary-Matriz] Peticionando data comercial de: {ticker}")
        return pd.DataFrame()

    def _normalize_data(self, raw_data: pd.DataFrame, ticker: str) -> pd.DataFrame:
        """
        Normaliza JSON o diccionarios arrojados por PyRofex hacia estructura Postgres
        """
        return pd.DataFrame()
