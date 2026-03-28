"""
refresh_earnings.py
===================
Traduce el workflow `.agents/workflows/refresh_earnings.md` a Python puro.

Pasos:
  1. Recibe la fecha (argumento CLI o interactivo) en formato YYYY-MM-DD.
  2. Copia los archivos Excel desde Descargas → withe-finance-ingest.
  3. Invoca ingest_cuenta_corriente_auto.ps1 para PESOS, DOLARES y DOLARES CABLE.
  4. Notifica al usuario que debe actualizar ganancias_realizadas.ipynb.

Uso:
  python refresh_earnings.py --fecha 2024-01-15
  python refresh_earnings.py                    # pide la fecha interactivamente
"""

import argparse
import logging
import re
import shutil
import subprocess
import sys
from datetime import datetime
from pathlib import Path

# ---------------------------------------------------------------------------
# Configuración de paths (espeja la configuración del script .ps1)
# ---------------------------------------------------------------------------
DOWNLOADS_DIR = Path(r"C:\Users\tomas\Downloads")
INGEST_DIR = Path(r"C:\Users\tomas\withe-finance-ingest")
PROJECT_DIR = Path(r"C:\Users\tomas\white_finance")
PS1_SCRIPT = PROJECT_DIR / "scripts" / "layers" / "AWS" / "raw" / "ingest" / "ingest_cuenta_corriente_auto.ps1"

MONEDAS: list[str] = ["PESOS", "DOLARES", "DOLARES CABLE"]

# ---------------------------------------------------------------------------
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - [%(levelname)s] - %(message)s",
)
logger = logging.getLogger("refresh_earnings")


# ---------------------------------------------------------------------------
# Paso 1: Validar la fecha
# ---------------------------------------------------------------------------
def solicitar_fecha(fecha_arg: str | None) -> str:
    """
    Devuelve la fecha validada en formato YYYY-MM-DD.
    Si no se recibe por argumento, la pide interactivamente.
    """
    patron = re.compile(r"^\d{4}-\d{2}-\d{2}$")

    if fecha_arg:
        fecha = fecha_arg.strip()
    else:
        fecha = input("Ingresa la fecha de la cuenta corriente (YYYY-MM-DD): ").strip()

    if not patron.match(fecha):
        logger.error(f"Formato de fecha inválido: '{fecha}'. Se esperaba YYYY-MM-DD.")
        sys.exit(1)

    try:
        datetime.strptime(fecha, "%Y-%m-%d")
    except ValueError as e:
        logger.error(f"Fecha inválida: {e}")
        sys.exit(1)

    logger.info(f"Fecha validada: {fecha}")
    return fecha


# ---------------------------------------------------------------------------
# Paso 2: Copiar archivos desde Descargas
# ---------------------------------------------------------------------------
def copiar_archivos(fecha: str) -> None:
    """
    Copia los Excel de cuentas corrientes desde Descargas al directorio de ingesta.
    Formato del nombre: 'Cuenta Corriente {MONEDA} {dd-MM-yy}.xlsx'
    """
    fecha_dt = datetime.strptime(fecha, "%Y-%m-%d")
    fecha_dd_mm_yy = fecha_dt.strftime("%d-%m-%y")

    INGEST_DIR.mkdir(parents=True, exist_ok=True)
    logger.info(f"Directorio de ingesta: {INGEST_DIR}")

    for moneda in MONEDAS:
        nombre_archivo = f"Cuenta Corriente {moneda} {fecha_dd_mm_yy}.xlsx"
        origen = DOWNLOADS_DIR / nombre_archivo
        destino = INGEST_DIR / nombre_archivo

        if origen.exists():
            shutil.copy2(origen, destino)
            logger.info(f"✅ Copiado: {nombre_archivo}")
        else:
            logger.warning(f"⚠️  No encontrado: {nombre_archivo}")


# ---------------------------------------------------------------------------
# Paso 3: Ejecutar el script PowerShell de ingesta por cada moneda
# ---------------------------------------------------------------------------
def procesar_moneda(fecha: str, moneda: str) -> bool:
    """
    Invoca ingest_cuenta_corriente_auto.ps1 para la moneda dada.
    Devuelve True si el proceso terminó con código 0.
    """
    logger.info(f"--- Procesando moneda: {moneda} ---")

    comando = [
        "powershell.exe",
        "-ExecutionPolicy", "Bypass",
        "-File", str(PS1_SCRIPT),
        "-fecha", fecha,
        "-moneda", moneda,
    ]

    try:
        resultado = subprocess.run(
            comando,
            cwd=str(PROJECT_DIR),
            capture_output=False,   # muestra stdout/stderr en tiempo real
            text=True,
        )

        if resultado.returncode == 0:
            logger.info(f"✅ Moneda '{moneda}' procesada correctamente.")
            return True
        else:
            logger.error(
                f"❌ Error procesando '{moneda}'. "
                f"Código de salida: {resultado.returncode}"
            )
            return False

    except FileNotFoundError:
        logger.error(
            "No se encontró powershell.exe. Verifica que PowerShell esté disponible en el PATH."
        )
        return False
    except Exception as exc:
        logger.error(f"Error inesperado ejecutando PowerShell para '{moneda}': {exc}")
        return False


def procesar_cuentas_corrientes(fecha: str) -> None:
    """Itera todas las monedas e invoca el script de ingesta para cada una."""
    resultados: dict[str, bool] = {}

    for moneda in MONEDAS:
        resultados[moneda] = procesar_moneda(fecha, moneda)

    logger.info("========================================")
    logger.info("Resumen de procesamiento:")
    for moneda, exito in resultados.items():
        estado = "✅ OK" if exito else "❌ FALLO"
        logger.info(f"  {moneda}: {estado}")
    logger.info("========================================")


# ---------------------------------------------------------------------------
# Paso 4: Notificación final
# ---------------------------------------------------------------------------
def notificar_finalizacion(fecha: str) -> None:
    """Informa al usuario que el proceso terminó y qué debe hacer a continuación."""
    mensaje = f"""
========================================================
✅  Proceso refresh_earnings completado para: {fecha}
========================================================

📌 PRÓXIMO PASO OBLIGATORIO:
   Ejecuta y actualiza el notebook:
   → notebooks/ganancias_realizadas.ipynb

   Esto es necesario para que los datos recién ingestados
   se reflejen en los reportes y visualizaciones.
========================================================
"""
    print(mensaje)
    logger.info("Notificación de finalización emitida.")


# ---------------------------------------------------------------------------
# Entrypoint
# ---------------------------------------------------------------------------
def main() -> None:
    parser = argparse.ArgumentParser(
        description="Actualiza las ganancias realizadas procesando cuentas corrientes."
    )
    parser.add_argument(
        "--fecha",
        type=str,
        default=None,
        help="Fecha de la cuenta corriente en formato YYYY-MM-DD (ej. 2024-01-15).",
    )
    args = parser.parse_args()

    # Paso 1
    fecha = solicitar_fecha(args.fecha)

    # Paso 2
    logger.info("=== PASO 2: Copiando archivos desde Descargas ===")
    copiar_archivos(fecha)

    # Paso 3
    logger.info("=== PASO 3: Procesando cuentas corrientes ===")
    procesar_cuentas_corrientes(fecha)

    # Paso 4
    notificar_finalizacion(fecha)


if __name__ == "__main__":
    main()
