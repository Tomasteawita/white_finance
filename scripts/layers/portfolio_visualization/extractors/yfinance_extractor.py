import pandas as pd
import yfinance as yf
from extractors.base_extractor import BaseExtractor


class YFinanceExtractor(BaseExtractor):
    """
    Subclase concreta de BaseExtractor para el proveedor Yahoo Finance.

    Lógica de mercado:
    - mercado == 'bCBA'          → busca '{especie}.BA', source = 'YFinance'
    - mercado == 'nYSE|nASDAQ'   → busca '{especie}' (USD), source = 'YFinance_USD'
    """

    def _authenticate(self) -> bool:
        # YFinance no requiere autenticación formal
        return True

    def _fetch_data(
        self,
        ticker: str,
        start_date: str = None,
        end_date: str = None,
        mercado: str = "bCBA",
    ) -> pd.DataFrame:
        """
        Descarga el histórico desde Yahoo Finance respetando el mercado de origen.

        Args:
            ticker:     Símbolo base (sin sufijo .BA / .US).
            start_date: Fecha de inicio 'YYYY-MM-DD'.
            end_date:   Fecha de fin    'YYYY-MM-DD'.
            mercado:    'bCBA' (BYMA) → agrega sufijo .BA; caso contrario usa ticker directo (USD).
        """
        if mercado == "bCBA":
            search_ticker = f"{ticker}.BA"
        else:
            search_ticker = ticker

        self.logger.info(f"[YFinance] Consultando '{search_ticker}' (mercado={mercado})")

        ticker_obj = yf.Ticker(search_ticker)

        if start_date and end_date:
            hist = ticker_obj.history(start=start_date, end=end_date)
        elif start_date:
            hist = ticker_obj.history(start=start_date)
        else:
            hist = ticker_obj.history(period="5y")

        return hist

    def _normalize_data(
        self, raw_data: pd.DataFrame, ticker: str, mercado: str = "bCBA"
    ) -> pd.DataFrame:
        """
        Normaliza el DataFrame de Yahoo Finance al esquema canónico de earnings.historical_prices.

        source:
            'YFinance'     → especie cotizada en pesos (BYMA).
            'YFinance_USD' → especie cotizada en dólares (NYSE/NASDAQ).
        """
        df = raw_data.reset_index()

        if "Date" not in df.columns:
            self.logger.warning(
                f"[YFinance] Columna 'Date' ausente al normalizar {ticker}"
            )
            return pd.DataFrame()

        # Normalizar timezone si existe
        if df["Date"].dt.tz is not None:
            df["date"] = df["Date"].dt.tz_localize(None).dt.date
        else:
            df["date"] = df["Date"].dt.date

        source = "YFinance" if mercado == "bCBA" else "YFinance_USD"
        df["source"] = source

        df.rename(
            columns={
                "Open": "open",
                "High": "high",
                "Low": "low",
                "Close": "close",
                "Volume": "volume",
            },
            inplace=True,
        )

        return df[["date", "open", "high", "low", "close", "volume", "source"]]
