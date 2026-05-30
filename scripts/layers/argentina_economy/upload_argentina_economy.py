"""
upload_argentina_economy.py
─────────────────────────
Script para crear el esquema 'argentina_economy' y subir datos de
RIPTE e IPC (mensual e interanual) a PostgreSQL.

Las variables de entorno se leen desde .env
"""

import os
import re
import logging
from pathlib import Path
from typing import Optional

import pandas as pd
import pdfplumber
from dotenv import load_dotenv
from sqlalchemy import create_engine, text

# ──────────────────────────────────────────────────────────────
# Logging (trazabilidad CNV)
# ──────────────────────────────────────────────────────────────
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
logger = logging.getLogger(__name__)

# ──────────────────────────────────────────────────────────────
# Paths & Constantes
# ──────────────────────────────────────────────────────────────
BASE_DIR = Path(__file__).resolve().parent.parent.parent
DATA_DIR = BASE_DIR / "data" / "argentina_economy"
RIPTE_PDF = DATA_DIR / "RIPTE mensual 1994-marzo 2026.pdf"
IPC_XLS = DATA_DIR / "IPC - historical 9 years.xls"
ENV_PATH = BASE_DIR / ".env"

MESES_MAP: dict[str, int] = {
    "enero": 1, "febrero": 2, "marzo": 3, "abril": 4,
    "mayo": 5, "junio": 6, "julio": 7, "agosto": 8,
    "septiembre": 9, "octubre": 10, "noviembre": 11, "diciembre": 12,
}

def get_engine():
    load_dotenv(ENV_PATH)
    # Por defecto usa las credenciales estándar si no hay específicas
    user = os.environ.get("POSTGRE_USER", "postgres")
    password = os.environ.get("POSTGRE_PASSWORD", "postgres")
    host = os.environ.get("POSTGRE_HOST", "localhost")
    port = os.environ.get("POSTGRE_PORT", "5432")
    db = os.environ.get("POSTGRE_DB", "postgres")
    
    conn_str = f"postgresql://{user}:{password}@{host}:{port}/{db}"
    logger.info(f"Conectando a DB {host}:{port}/{db}")
    return create_engine(conn_str)

def init_db(engine):
    with engine.begin() as conn:
        logger.info("Creando esquema argentina_economy...")
        conn.execute(text("CREATE SCHEMA IF NOT EXISTS argentina_economy;"))

def extract_ipc_data(xls_path: Path) -> pd.DataFrame:
    """Extrae las series de variación mensual e interanual del IPC Nacional.
    Lee directamente de la hoja 'to_antigravity' preprocesada.
    Retorna un DataFrame con ['date', 'monthly', 'year_to_year'].
    """
    if not xls_path.exists():
        raise FileNotFoundError(f"No se encontró el Excel IPC en: {xls_path}")

    logger.info(f"Extrayendo IPC desde hoja 'to_antigravity' en: {xls_path.name}")
    
    df_ipc = pd.read_excel(str(xls_path), sheet_name="to_antigravity")
    
    # Limpieza y formateo
    df_ipc.columns = ["date", "monthly", "year_to_year"]
    df_ipc["date"] = pd.to_datetime(df_ipc["date"], errors="coerce")
    df_ipc["monthly"] = pd.to_numeric(df_ipc["monthly"], errors="coerce")
    df_ipc["year_to_year"] = pd.to_numeric(df_ipc["year_to_year"], errors="coerce")
    
    df_ipc = df_ipc.dropna(subset=["date"]).sort_values("date").reset_index(drop=True)
    
    logger.info(f"IPC extraído: {len(df_ipc)} registros")
    return df_ipc

def _parse_monto(raw: str) -> Optional[float]:
    """Parsea monto argentino a float."""
    try:
        cleaned = raw.replace("$", "").strip()
        cleaned = cleaned.replace(".", "").replace(",", ".")
        return float(cleaned)
    except (ValueError, AttributeError):
        return None

def _parse_porcentaje(raw: str) -> Optional[float]:
    """Parsea porcentaje a float, ej: '2,1%' -> 2.1"""
    try:
        cleaned = raw.replace("%", "").strip()
        cleaned = cleaned.replace(",", ".")
        if cleaned == "-":
            return 0.0 # O None, depende de convención, pero "-" es 0 var
        return float(cleaned)
    except (ValueError, AttributeError):
        return None

def extract_ripte_data(pdf_path: Path) -> pd.DataFrame:
    """Extrae RIPTE con monto y variación porcentual.
    Retorna un DataFrame con ['date', 'amount_ripte', 'percentage_of_variation'].
    """
    if not pdf_path.exists():
        raise FileNotFoundError(f"No se encontró el PDF RIPTE en: {pdf_path}")

    logger.info(f"Extrayendo RIPTE desde: {pdf_path.name}")
    
    records: list[dict] = []
    current_year: Optional[int] = None

    # Regex para capturar: Mes, Monto y Porcentaje de variación
    # Ej: "Agosto $ 893,00 2,1% 102,07 102,07"
    # Ej: "Julio $ 874,87 - 100,00 100,00"
    line_pattern = re.compile(
        r"^(Enero|Febrero|Marzo|Abril|Mayo|Junio|Julio|Agosto|Septiembre|Octubre|Noviembre|Diciembre)"
        r"\s+(\$\s*[\d.,]+)"
        r"\s+(-|[\d.,]+%)",
        re.IGNORECASE,
    )
    year_pattern = re.compile(r"^(\d{4})$")

    with pdfplumber.open(str(pdf_path)) as pdf:
        for page in pdf.pages:
            text = page.extract_text()
            if not text:
                continue

            for line in text.split("\n"):
                line = line.strip()

                year_match = year_pattern.match(line)
                if year_match:
                    current_year = int(year_match.group(1))
                    continue

                data_match = line_pattern.match(line)
                if data_match and current_year is not None:
                    mes_str = data_match.group(1).lower()
                    monto_str = data_match.group(2)
                    var_str = data_match.group(3)
                    
                    mes_num = MESES_MAP.get(mes_str)
                    monto = _parse_monto(monto_str)
                    var_pct = _parse_porcentaje(var_str)

                    if mes_num is not None and monto is not None:
                        periodo = pd.Timestamp(year=current_year, month=mes_num, day=1)
                        records.append({
                            "date": periodo,
                            "amount_ripte": monto,
                            "percentage_of_variation": var_pct
                        })

    df = pd.DataFrame(records)
    df = df.sort_values("date").drop_duplicates(subset=["date"], keep="last").reset_index(drop=True)
    
    logger.info(f"RIPTE extraído: {len(df)} registros")
    return df

def main():
    logger.info("Iniciando proceso de carga a PostgreSQL")
    
    engine = get_engine()
    init_db(engine)
    
    df_ipc = extract_ipc_data(IPC_XLS)
    df_ripte = extract_ripte_data(RIPTE_PDF)
    
    with engine.begin() as conn:
        logger.info("Subiendo tabla ipc_monthly_year_on_year...")
        df_ipc.to_sql(
            name="ipc_monthly_year_on_year",
            con=conn,
            schema="argentina_economy",
            if_exists="replace",
            index=False
        )
        
        logger.info("Subiendo tabla ripte...")
        df_ripte.to_sql(
            name="ripte",
            con=conn,
            schema="argentina_economy",
            if_exists="replace",
            index=False
        )
        
    logger.info("Carga finalizada con éxito.")

if __name__ == "__main__":
    main()
