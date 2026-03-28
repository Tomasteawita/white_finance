import datetime

import pandas as pd
from extractors.base_extractor import BaseExtractor
from extractors.iol_manager import IOLManager


class IOLExtractor(BaseExtractor):
    """
    Subclase concreta del Template Method apuntando a la InvertirOnline REST API.
    Aprovecha iol_manager.py para gestionar la autenticación OAuth2 persistente.

    Lógica de mercado:
    - mercado == 'bCBA'        → consulta directamente mercado='bCBA'.
    - mercado != 'bCBA'        → prueba mercado='nYSE'; si falla o vacío, prueba 'nASDAQ'.
    source resultante: f'API_IOL_{mercado_efectivo}'
    """

    def __init__(self):
        super().__init__()
        self.client = IOLManager()

    def _authenticate(self) -> bool:
        """Invoca el motor validatorio de tokens en el IOLManager."""
        self.logger.info("[IOL] Autorizando identidad OAuth2...")
        try:
            headers = self.client.get_headers()
            if headers:
                return True
        except Exception as e:
            self.logger.error(
                f"[IOL] Bloqueo 401 en autenticación (Requiere habilitación broker): {e}"
            )
        return False

    def _fetch_data(
        self,
        ticker: str,
        start_date: str = None,
        end_date: str = None,
        mercado: str = "bCBA",
    ) -> pd.DataFrame:
        """
        Extrae el histórico a través del wrapper get_serie_historica.

        Si mercado es 'bCBA' consulta BYMA directamente.
        Si es otro mercado (USA), prueba nYSE primero y cae en nASDAQ.
        El mercado efectivamente utilizado queda registrado en el atributo
        _last_mercado para que _normalize_data pueda leer el source correcto.

        Args:
            ticker:     Símbolo base.
            start_date: Fecha de inicio 'YYYY-MM-DD'. Default: últimos 5 años.
            end_date:   Fecha de fin    'YYYY-MM-DD'. Default: hoy.
            mercado:    'bCBA' (BYMA) o cualquier otro valor (USA).
        """
        fecha_hasta = end_date or datetime.datetime.now().strftime("%Y-%m-%d")
        fecha_desde = start_date or (
            datetime.datetime.now() - datetime.timedelta(days=365 * 5)
        ).strftime("%Y-%m-%d")

        if mercado == "bCBA":
            mercados_a_probar = ["bCBA"]
        else:
            mercados_a_probar = ["nYSE", "nASDAQ"]

        for mkt in mercados_a_probar:
            try:
                self.logger.info(
                    f"[IOL] Consultando {ticker} | mercado={mkt} | {fecha_desde} → {fecha_hasta}"
                )
                res = self.client.get_serie_historica(
                    mercado=mkt,
                    simbolo=ticker,
                    fecha_desde=fecha_desde,
                    fecha_hasta=fecha_hasta,
                    ajustada="ajustada",
                )
                if res:  # lista no vacía
                    df = pd.DataFrame(res)
                    # Guardamos el mercado efectivo para usar en _normalize_data
                    self._last_mercado = mkt
                    return df
                else:
                    self.logger.warning(
                        f"[IOL] Respuesta vacía para {ticker} en mercado={mkt}. "
                        f"Probando siguiente fallback..."
                    )
            except Exception as e:
                self.logger.warning(
                    f"[IOL] Error en {ticker} | mercado={mkt}: {e}. "
                    f"Probando siguiente fallback..."
                )

        self.logger.error(
            f"[IOL] Agotados todos los mercados para {ticker}: {mercados_a_probar}"
        )
        self._last_mercado = None
        return pd.DataFrame()

    def _normalize_data(
        self, raw_data: pd.DataFrame, ticker: str, mercado: str = "bCBA"
    ) -> pd.DataFrame:
        """
        IOL devuelve campos como:
            "fechaHora": "2024-03-22T00:00:00"
            "apertura", "maximo", "minimo", "ultimoPrecio", "volumen"

        source = f'API_IOL_{mercado_efectivo}'
        """
        if raw_data.empty:
            return pd.DataFrame()

        df = raw_data.copy()

        if "fechaHora" not in df.columns:
            self.logger.warning(
                f"[IOL] Estructura JSON imprevista, sin 'fechaHora': {df.columns.tolist()}"
            )
            return pd.DataFrame()

        df["date"] = pd.to_datetime(df["fechaHora"]).dt.date

        col_map = {
            "apertura": "open",
            "maximo": "high",
            "minimo": "low",
            "ultimoPrecio": "close",
            "volumen": "volume",
        }
        df.rename(columns=col_map, inplace=True)

        # Usar el mercado efectivo (guardado durante _fetch_data) para el source
        mercado_efectivo = getattr(self, "_last_mercado", None) or mercado
        df["source"] = f"API_IOL_{mercado_efectivo}"

        expected = ["date", "open", "high", "low", "close", "volume", "source"]
        for col in expected:
            if col not in df.columns:
                df[col] = None

        return df[expected]
