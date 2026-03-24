"""
Módulo IOLManager: Gestor de conexiones con la API de InvertirOnline.
Implementa el ciclo de vida de Bearer Tokens y Refresh Tokens para 
conexiones persistentes y seguras hacia el mercado local.
Incluye persistencia de sesión local (.iol_token_cache.json)
"""

import os
import json
import time
import requests
import logging
from dotenv import load_dotenv
from typing import Optional, Dict, Any

logger = logging.getLogger("IOLManager")

class IOLManager:
    """
    Gestor de autenticación y peticiones para la API de InvertirOnline (IOL).
    Maneja dinámicamente el ciclo de vida del Bearer token y el Refresh token.
    Implementación robusta aplicando asimetría positiva en el manejo de caídas.
    """
    TOKEN_URL = "https://api.invertironline.com/token"
    # Archivo caché ubicado en el mismo directorio del script
    CACHE_FILE = os.path.join(os.path.dirname(__file__), ".iol_token_cache.json")

    def __init__(self):
        # Cargar variables desde el archivo .env de forma explícita
        load_dotenv()
        
        # Validación defensiva limpiando posibles saltos y comillas indeseadas
        self._username = os.getenv("USERNAME_IOL", "").strip("'\"").strip()
        self._password = os.getenv("PASSWORD_IOL", "").strip("'\"").strip()
        
        if not self._username or not self._password:
            raise ValueError("Las credenciales USERNAME_IOL o PASSWORD_IOL no se encontraron en .env")

        self._access_token: Optional[str] = None
        self._refresh_token: Optional[str] = None
        self._token_expiry: float = 0.0
        
        # Intentamos recuperar la sesión persistida para evitar llamadas duplicadas
        self._load_cached_token()

    def _load_cached_token(self) -> None:
        """
        Lee el archivo de caché local y recupera los tokens si existen y son recuperables.
        """
        if os.path.exists(self.CACHE_FILE):
            try:
                with open(self.CACHE_FILE, "r", encoding="utf-8") as f:
                    data = json.load(f)
                    
                self._access_token = data.get("access_token")
                self._refresh_token = data.get("refresh_token")
                self._token_expiry = data.get("token_expiry", 0.0)
                logger.info("Sesión IOL cargada desde caché local.")
            except Exception as e:
                logger.warning(f"Error leyendo caché de tokens IOL ({e}). Se re-autenticará la sesión.")

    def _save_cached_token(self) -> None:
        """
        Persiste los tokens actuales en el disco para reuso en futuros scripts.
        """
        try:
            data = {
                "access_token": self._access_token,
                "refresh_token": self._refresh_token,
                "token_expiry": self._token_expiry
            }
            with open(self.CACHE_FILE, "w", encoding="utf-8") as f:
                json.dump(data, f)
        except Exception as e:
            logger.error(f"Falla silenciosa guardando caché de tokens IOL localmente: {e}")

    def _authenticate(self) -> None:
        """
        Ejecuta la petición inicial para solicitar un token mediante grant_type = password.
        Falla ruidosamente si las credenciales son inválidas.
        """
        payload = {
            "username": self._username,
            "password": self._password,
            "grant_type": "password"
        }
        logger.info("Solicitando nuevo token de acceso a IOL...")
        self._request_token(payload)

    def _refresh(self) -> None:
        """
        Renueva el Bearer token utilizando el refresh_token guardado.
        """
        if not self._refresh_token:
            logger.warning("No se cuenta con refresh_token. Derivando a autenticación completa.")
            self._authenticate()
            return

        payload = {
            "refresh_token": self._refresh_token,
            "grant_type": "refresh_token"
        }
        logger.info("Refrescando sesión actual de IOL...")
        try:
            self._request_token(payload)
        except Exception as e:
            logger.error(f"Falla crítica al refrescar el token: {str(e)}. Forzando re-autenticación.")
            self._authenticate()

    def _request_token(self, payload: Dict[str, str]) -> None:
        """
        Método subyacente que unifica la obtención de tokens a IOL y persiste en caché.
        """
        headers = {
            "Content-Type": "application/x-www-form-urlencoded"
        }
        
        response = requests.post(self.TOKEN_URL, data=payload, headers=headers)
        
        if response.status_code != 200:
            logger.error(f"Status no satisfactorio desde IOL: {response.status_code} | {response.text}")
            response.raise_for_status()
            
        data = response.json()
        self._access_token = data.get("access_token")
        self._refresh_token = data.get("refresh_token")
        
        # Prevención de Tail Risk: Usaremos 60 segundos de margen antes de la expiración oficial
        expires_in = int(data.get("expires_in", 900))
        self._token_expiry = time.time() + expires_in - 60
        
        logger.info("Tokens IOL actualizados con éxito en memoria.")
        self._save_cached_token()

    def get_headers(self) -> Dict[str, str]:
        """
        Genera y retorna los headers requeridos para peticiones a la API IOL.
        Controla expiración en memoria/caché y re-acredita automáticamente.
        """
        # Validar si el token actual está vencido o falta
        if not self._access_token or time.time() >= self._token_expiry:
            if not self._refresh_token:
                self._authenticate()
            else:
                self._refresh()

        return {
            "Authorization": f"Bearer {self._access_token}",
            "Accept": "application/json"
        }

    def get_data(self, endpoint: str, params: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
        """
        Firma y ejecuta un request GET hacia IOL con el Authorization dinámico.
        """
        if not endpoint.startswith("http"):
            domain = "https://api.invertironline.com"
            endpoint = f"{domain}{endpoint}" if endpoint.startswith("/") else f"{domain}/{endpoint}"
            
        headers = self.get_headers()
        
        response = requests.get(endpoint, headers=headers, params=params)
        
        if response.status_code != 200:
            logger.error(f"Falla peticionando a {endpoint}. Status: {response.status_code} | Body: {response.text}")
            response.raise_for_status()
            
        return response.json()

    # --------------------------------------------------------------------------------
    # Mapeo de Endpoints de la API Titulos / Cotizaciones extraídos del Swagger
    # --------------------------------------------------------------------------------

    def get_fci(self) -> Dict[str, Any]:
        """ GET /api/v2/Titulos/FCI """
        return self.get_data("/api/v2/Titulos/FCI")

    def get_fci_by_simbolo(self, simbolo: str) -> Dict[str, Any]:
        """ GET /api/v2/Titulos/FCI/{simbolo} """
        return self.get_data(f"/api/v2/Titulos/FCI/{simbolo}")

    def get_fci_tipos_fondos(self) -> Dict[str, Any]:
        """ GET /api/v2/Titulos/FCI/TipoFondos """
        return self.get_data("/api/v2/Titulos/FCI/TipoFondos")

    def get_cotizacion_mep(self, simbolo: str) -> Dict[str, Any]:
        """ GET /api/v2/Cotizaciones/MEP/{simbolo} """
        return self.get_data(f"/api/v2/Cotizaciones/MEP/{simbolo}")

    def get_fci_administradoras(self) -> Dict[str, Any]:
        """ GET /api/v2/Titulos/FCI/Administradoras """
        return self.get_data("/api/v2/Titulos/FCI/Administradoras")

    def get_titulo(self, mercado: str, simbolo: str) -> Dict[str, Any]:
        """ GET /api/v2/{mercado}/Titulos/{simbolo} """
        return self.get_data(f"/api/v2/{mercado}/Titulos/{simbolo}")

    def get_opciones(self, mercado: str, simbolo: str) -> Dict[str, Any]:
        """ GET /api/v2/{mercado}/Titulos/{simbolo}/Opciones """
        return self.get_data(f"/api/v2/{mercado}/Titulos/{simbolo}/Opciones")

    def get_cotizacion_instrumentos(self, pais: str) -> Dict[str, Any]:
        """ GET /api/v2/{pais}/Titulos/Cotizacion/Instrumentos """
        return self.get_data(f"/api/v2/{pais}/Titulos/Cotizacion/Instrumentos")

    def get_cotizaciones_todos(self, instrumento: str, pais: str) -> Dict[str, Any]:
        """ GET /api/v2/Cotizaciones/{Instrumento}/{Pais}/Todos """
        return self.get_data(f"/api/v2/Cotizaciones/{instrumento}/{pais}/Todos")

    def get_cotizaciones_panel(self, instrumento: str, panel: str, pais: str) -> Dict[str, Any]:
        """ GET /api/v2/Cotizaciones/{Instrumento}/{Panel}/{Pais} """
        return self.get_data(f"/api/v2/Cotizaciones/{instrumento}/{panel}/{pais}")

    def get_cotizacion_detalle(self, mercado: str, simbolo: str) -> Dict[str, Any]:
        """ GET /api/v2/{mercado}/Titulos/{simbolo}/CotizacionDetalle """
        return self.get_data(f"/api/v2/{mercado}/Titulos/{simbolo}/CotizacionDetalle")

    def get_cotizaciones_orleans_todos(self, instrumento: str, pais: str) -> Dict[str, Any]:
        """ GET /api/v2/cotizaciones-orleans/{Instrumento}/{Pais}/Todos """
        return self.get_data(f"/api/v2/cotizaciones-orleans/{instrumento}/{pais}/Todos")

    def get_paneles(self, pais: str, instrumento: str) -> Dict[str, Any]:
        """ GET /api/v2/{pais}/Titulos/Cotizacion/Paneles/{instrumento} """
        return self.get_data(f"/api/v2/{pais}/Titulos/Cotizacion/Paneles/{instrumento}")

    def get_cotizaciones_orleans_operables(self, instrumento: str, pais: str) -> Dict[str, Any]:
        """ GET /api/v2/cotizaciones-orleans/{Instrumento}/{Pais}/Operables """
        return self.get_data(f"/api/v2/cotizaciones-orleans/{instrumento}/{pais}/Operables")

    def get_cotizacion(self, mercado: str, simbolo: str) -> Dict[str, Any]:
        """ GET /api/v2/{Mercado}/Titulos/{Simbolo}/Cotizacion """
        return self.get_data(f"/api/v2/{mercado}/Titulos/{simbolo}/Cotizacion")

    def get_cotizaciones_orleans_panel_todos(self, instrumento: str, pais: str) -> Dict[str, Any]:
        """ GET /api/v2/cotizaciones-orleans-panel/{Instrumento}/{Pais}/Todos """
        return self.get_data(f"/api/v2/cotizaciones-orleans-panel/{instrumento}/{pais}/Todos")

    def get_fci_tipo_fondos_por_admin(self, administradora: str) -> Dict[str, Any]:
        """ GET /api/v2/Titulos/FCI/Administradoras/{administradora}/TipoFondos """
        return self.get_data(f"/api/v2/Titulos/FCI/Administradoras/{administradora}/TipoFondos")

    def get_cotizaciones_orleans_panel_operables(self, instrumento: str, pais: str) -> Dict[str, Any]:
        """ GET /api/v2/cotizaciones-orleans-panel/{Instrumento}/{Pais}/Operables """
        return self.get_data(f"/api/v2/cotizaciones-orleans-panel/{instrumento}/{pais}/Operables")

    def get_cotizacion_detalle_mobile(self, mercado: str, simbolo: str, plazo: str) -> Dict[str, Any]:
        """ GET /api/v2/{mercado}/Titulos/{simbolo}/CotizacionDetalleMobile/{plazo} """
        return self.get_data(f"/api/v2/{mercado}/Titulos/{simbolo}/CotizacionDetalleMobile/{plazo}")

    def get_fci_por_admin_y_tipo(self, administradora: str, tipo_fondo: str) -> Dict[str, Any]:
        """ GET /api/v2/Titulos/FCI/Administradoras/{administradora}/TipoFondos/{tipoFondo} """
        return self.get_data(f"/api/v2/Titulos/FCI/Administradoras/{administradora}/TipoFondos/{tipo_fondo}")

    def get_serie_historica(self, mercado: str, simbolo: str, fecha_desde: str, fecha_hasta: str, ajustada: str) -> Dict[str, Any]:
        """ GET /api/v2/{mercado}/Titulos/{simbolo}/Cotizacion/seriehistorica/{fechaDesde}/{fechaHasta}/{ajustada} """
        return self.get_data(f"/api/v2/{mercado}/Titulos/{simbolo}/Cotizacion/seriehistorica/{fecha_desde}/{fecha_hasta}/{ajustada}")

# Ejemplo simplificado de inicialización
if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
    try:
        iol = IOLManager()
        # Llamar múltiples veces solo requiere hacer la auth la primera ocasión o si el archivo existe leerlo
        print("Obteniendo headers iteración 1...")
        headers = iol.get_headers()
        print(f"Header 1 obtenido: {headers['Authorization'][:20]}...")
        
        print("\nObteniendo headers iteración 2 (debe usar memoria/caché)...")
        headers = iol.get_headers()
        print(f"Header 2 obtenido: {headers['Authorization'][:20]}...")
    except Exception as e:
         print(f"Error testeando la gestión IOL: {e}")
