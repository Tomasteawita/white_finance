import logging
import json
import sys
import os

sys.path.append(r"c:\Users\tomas\white_finance\scripts\layers\portfolio_visualization")
from extractors.iol_manager import IOLManager

logging.basicConfig(level=logging.INFO, format="%(asctime)s - [%(levelname)s] - %(message)s")

def test_iol():
    logger = logging.getLogger("TestIOL")
    logger.info("Instanciando IOLManager...")
    manager = IOLManager()
    
    logger.info("Probando Autenticación OAuth2 (get_headers)...")
    try:
        headers = manager.get_headers()
        if not headers:
            logger.error("No se obtuvieron headers. Falla en autenticación.")
            return
        logger.info(f"Headers obtenidos exitosamente (Bearer Token generado).")
    except Exception as e:
        logger.error(f"Falla crítica en Auth: {e}")
        return
        
    logger.info("Probando Consulta de Cotización Histórica Estándar (Ej: GGAL)...")
    try:
        from datetime import datetime, timedelta
        hasta = datetime.now().strftime("%Y-%m-%d")
        desde = (datetime.now() - timedelta(days=30)).strftime("%Y-%m-%d")
        
        hist = manager.get_serie_historica(
            mercado="bCBA",
            simbolo="GGAL",
            fecha_desde=desde,
            fecha_hasta=hasta,
            ajustada="ajustada"
        )
        
        if hist and len(hist) > 0:
            logger.info(f"✅ ¡ÉXITO! Se obtuvieron {len(hist)} registros para GGAL.")
            logger.info(f"Muestra del primer registro: {json.dumps(hist[0], indent=2)}")
        else:
            logger.warning("Llamada exitosa pero sin datos devueltos para GGAL en las fechas provistas.")
            
    except Exception as e:
        logger.error(f"Falla crítica consultando serie GGAL: {e}")

if __name__ == "__main__":
    test_iol()
