"""
refresh_earnings.py (Pure Python Version)
=========================================
Lógica de ingesta de archivos Excel de cuentas corrientes a AWS S3.
Valida, sube a S3, dispara la Step Function y descarga el resultado,
sin depender de PowerShell. Todo usando boto3.
"""

import argparse
import logging
import re
import sys
import os
import json
import time
from datetime import datetime
from pathlib import Path
import boto3

# ---------------------------------------------------------------------------
# Configuración de paths dinámicos
# ---------------------------------------------------------------------------
PROJECT_DIR = Path(__file__).resolve().parent.parent.parent.parent
INGEST_DIR = PROJECT_DIR / "data" / "in"
ANALYTICS_DIR = PROJECT_DIR / "data" / "analytics"

# Agregamos el path de los validators para poder importarlos dinámicamente
VALIDATORS_PATH = PROJECT_DIR / "scripts" / "layers" / "AWS" / "raw" / "ingest" / "validators"
sys.path.append(str(VALIDATORS_PATH))
from main import main as validator_main

MONEDAS: list[str] = ["PESOS", "DOLARES", "DOLARES CABLE"]

# AWS Config
S3_BUCKET = "withefinance-raw"
S3_PREFIX = "data/in"
STATE_MACHINE_ARN = "arn:aws:states:us-east-2:515966533232:stateMachine:WitheFinance-Historical-Profits"

# ---------------------------------------------------------------------------
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - [%(levelname)s] - %(message)s",
)
logger = logging.getLogger("refresh_earnings")


def solicitar_fecha(fecha_arg: str | None) -> str:
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

    return fecha


def validar_archivos_existen(fecha: str) -> None:
    fecha_dt = datetime.strptime(fecha, "%Y-%m-%d")
    fecha_dd_mm_yy = fecha_dt.strftime("%d-%m-%y")

    INGEST_DIR.mkdir(parents=True, exist_ok=True)
    ANALYTICS_DIR.mkdir(parents=True, exist_ok=True)
    
    # Revisamos que los excels existan
    missing = []
    for moneda in MONEDAS:
        nombre_archivo = f"Cuenta Corriente {moneda} {fecha_dd_mm_yy}.xlsx"
        if not (INGEST_DIR / nombre_archivo).exists():
            missing.append(nombre_archivo)
            
    if missing:
        logger.warning(f"⚠️ Faltan los siguientes archivos en {INGEST_DIR}: {missing}")
        logger.warning("Si estás ejecutando desde Streamlit, recordá primero descargar los excels.")
        # Podríamos lanzar excepcion, pero por ahora solo advertimos (quizas solo queriamos correr los que estan)

def procesar_moneda(fecha: str, moneda: str, s3_client, sf_client) -> bool:
    logger.info(f"--- Procesando moneda: {moneda} ---")
    fecha_dt = datetime.strptime(fecha, "%Y-%m-%d")
    fecha_dd_mm_yy = fecha_dt.strftime("%d-%m-%y")
    fecha_yyyy_mm_dd = fecha_dt.strftime("%Y%m%d")
    
    excel_filename = f"Cuenta Corriente {moneda} {fecha_dd_mm_yy}.xlsx"
    excel_path = INGEST_DIR / excel_filename
    
    if not excel_path.exists():
        logger.error(f"No se encontró el archivo '{excel_filename}' en {INGEST_DIR}")
        return False

    if moneda == "PESOS":
        csv_filename = f"cuenta_corriente-{fecha_yyyy_mm_dd}.csv"
        val_name = "cuenta_corriente"
    elif moneda == "DOLARES":
        csv_filename = f"cuenta_corriente_dolares-{fecha_yyyy_mm_dd}.csv"
        val_name = "cuenta_corriente_dolares"
    else:
        csv_filename = f"cuenta_corriente_dolares_cable-{fecha_yyyy_mm_dd}.csv"
        val_name = "cuenta_corriente_dolares_cable"

    csv_path = INGEST_DIR / csv_filename
    
    # 1. Ejecutar validación
    logger.info(f"[PYTHON] Ejecutando validador '{val_name}'...")
    try:
        validator_main(
            file_path=str(excel_path),
            output_path=str(csv_path),
            validator_name=val_name
        )
    except Exception as e:
        logger.error(f"Error en validación: {e}")
        return False
        
    if not csv_path.exists():
        logger.error(f"El archivo CSV '{csv_filename}' no fue creado.")
        return False

    # 2. Subir a S3
    logger.info("[S3] Subiendo CSV a S3...")
    s3_key = f"{S3_PREFIX}/{csv_filename}"
    try:
        s3_client.upload_file(str(csv_path), S3_BUCKET, s3_key)
        logger.info(f"[OK] Sincronización exitosa: s3://{S3_BUCKET}/{s3_key}")
    except Exception as e:
        logger.error(f"Error subiendo a S3: {e}")
        return False

    # 3. Preparar JSON y ejecutar Step Function
    logger.info("[STEPFUNC] Iniciando Step Function...")
    input_payload = {
        "Records": [
            {
                "s3": {
                    "bucket": {"name": S3_BUCKET},
                    "object": {"key": s3_key}
                }
            }
        ]
    }
    
    try:
        response = sf_client.start_execution(
            stateMachineArn=STATE_MACHINE_ARN,
            input=json.dumps(input_payload)
        )
        logger.info(f"[OK] Step Function iniciada. ARN: {response['executionArn']}")
    except Exception as e:
        logger.error(f"Fallo el inicio de la Step Function: {e}")
        return False

    # 4. Limpiar temporales
    try:
        csv_path.unlink(missing_ok=True)
        excel_path.unlink(missing_ok=True)
        logger.info("[CLEANUP] Archivos locales eliminados.")
    except Exception as e:
        logger.warning(f"No se pudieron limpiar archivos temporales: {e}")

    # 5. Esperar procesamiento (30s igual que PowerShell)
    logger.info("[WAIT] Esperando 30 segundos para que la Step Function procese...")
    time.sleep(30)

    # 6. Descargar resultados
    logger.info("[DOWNLOAD] Descargando archivo histórico actualizado...")
    
    try:
        if moneda == "PESOS":
            hist_name = "cuenta_corriente_historico.csv"
            # Adicional para pesos
            s3_client.download_file("whitefinance-analytics", "profit.csv", str(ANALYTICS_DIR / "profit.csv"))
        elif moneda == "DOLARES":
            hist_name = "cuenta_corriente_dolares_historico.csv"
        else:
            hist_name = "cuenta_corriente_dolares_cable_historico.csv"
            
        s3_client.download_file("withefinance-integrated", f"cuenta_corriente_historico/{hist_name}", str(ANALYTICS_DIR / hist_name))
        logger.info(f"[OK] Proceso completado para {moneda}")
        return True
        
    except Exception as e:
        logger.error(f"Error descargando archivos finales: {e}")
        return False

def procesar_cuentas_corrientes(fecha: str) -> None:
    try:
        s3_client = boto3.client('s3')
        sf_client = boto3.client('stepfunctions')
    except Exception as e:
        logger.error(f"Error inicializando clientes AWS (verificar credenciales): {e}")
        sys.exit(1)

    resultados: dict[str, bool] = {}

    for moneda in MONEDAS:
        resultados[moneda] = procesar_moneda(fecha, moneda, s3_client, sf_client)

    logger.info("========================================")
    logger.info("Resumen de procesamiento:")
    for moneda, exito in resultados.items():
        estado = "✅ OK" if exito else "❌ FALLO"
        logger.info(f"  {moneda}: {estado}")
    logger.info("========================================")


def notificar_finalizacion(fecha: str) -> None:
    mensaje = f"""
========================================================
✅  Proceso refresh_earnings completado para: {fecha}
========================================================
"""
    print(mensaje)


def main(fecha_arg: str = None) -> None:
    fecha = solicitar_fecha(fecha_arg)

    logger.info("=== PASO 1: Validando Archivos ===")
    validar_archivos_existen(fecha)

    logger.info("=== PASO 2: Procesando cuentas corrientes ===")
    procesar_cuentas_corrientes(fecha)

    notificar_finalizacion(fecha)


if __name__ == "__main__":
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
    
    main(args.fecha)
