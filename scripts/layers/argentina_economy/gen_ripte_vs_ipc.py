"""
gen_ripte_vs_ipc.py
───────────────────
Genera un CSV con la evolución del RIPTE real vs. un RIPTE simulado que
sigue al IPC Nacional General mes a mes, junto con las variaciones
porcentuales mensuales de ambos.

Fuentes:
  - Base de datos PostgreSQL: argentina_economy.ripte
  - Base de datos PostgreSQL: argentina_economy.ipc_monthly_year_on_year

Salida: CSV en data/argentina_economy/ripte_vs_ipc_analysis.csv
"""

import os
import logging
from pathlib import Path

import pandas as pd
from dotenv import load_dotenv
from sqlalchemy import create_engine

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
# Paths
# ──────────────────────────────────────────────────────────────
BASE_DIR = Path(__file__).resolve().parent.parent.parent.parent
DATA_DIR = BASE_DIR / "data" / "argentina_economy"
OUTPUT_CSV = DATA_DIR / "ripte_vs_ipc_analysis.csv"
ENV_PATH = BASE_DIR / ".env"

def get_engine():
    load_dotenv(ENV_PATH)
    user = os.environ.get("POSTGRE_USER", "postgres")
    password = os.environ.get("POSTGRE_PASSWORD", "postgres")
    host = os.environ.get("POSTGRE_HOST", "localhost")
    port = os.environ.get("POSTGRE_PORT", "5432")
    db = os.environ.get("POSTGRE_DB", "postgres")
    
    conn_str = f"postgresql://{user}:{password}@{host}:{port}/{db}"
    return create_engine(conn_str)

def get_data(engine) -> tuple[pd.DataFrame, pd.DataFrame]:
    """Extrae las tablas de PostgreSQL."""
    logger.info("Extrayendo datos de la base de datos...")
    query_ripte = "SELECT date, amount_ripte, percentage_of_variation FROM argentina_economy.ripte"
    query_ipc = "SELECT date, monthly, year_to_year FROM argentina_economy.ipc_monthly_year_on_year"
    
    df_ripte = pd.read_sql(query_ripte, engine)
    df_ipc = pd.read_sql(query_ipc, engine)
    
    df_ripte["date"] = pd.to_datetime(df_ripte["date"])
    df_ipc["date"] = pd.to_datetime(df_ipc["date"])
    
    return df_ripte, df_ipc

def build_analysis_csv(df_ripte: pd.DataFrame, df_ipc: pd.DataFrame, output_path: Path) -> pd.DataFrame:
    # Merge por período
    df = pd.merge(df_ripte, df_ipc, on="date", how="inner")
    df = df.sort_values("date").reset_index(drop=True)

    if df.empty:
        raise ValueError("No hay períodos comunes entre RIPTE e IPC.")

    logger.info(
        f"Períodos comunes: {len(df)} | "
        f"Rango: {df['date'].min():%Y-%m} a {df['date'].max():%Y-%m}"
    )

    ripte_base = df["amount_ripte"].iloc[0]

    logger.info(
        f"Período base: {df['date'].iloc[0]:%Y-%m} | "
        f"RIPTE base: ${ripte_base:,.2f}"
    )

    # ── RIPTE simulado siguiendo al IPC ──
    # Reconstruimos el IPC acumulado usando el monthly variation
    df["monthly_factor"] = 1 + (df["monthly"].fillna(0) / 100)
    # El primer mes es la base, así que no se le aplica la inflación de ese mes sobre sí mismo
    df.loc[0, "monthly_factor"] = 1.0 
    df["cumulative_ipc_factor"] = df["monthly_factor"].cumprod()
    
    df["monto_ripte_si_ipc"] = ripte_base * df["cumulative_ipc_factor"]

    # ── Brecha porcentual RIPTE real vs simulado IPC ──
    df["brecha_pct"] = ((df["amount_ripte"] / df["monto_ripte_si_ipc"]) - 1) * 100

    # ── Formatear salida ──
    df_output = pd.DataFrame({
        "Período": df["date"].dt.strftime("%Y-%m"),
        "Monto en $ RIPTE": df["amount_ripte"].round(2),
        "Monto en $ si los sueldos seguían al IPC": df["monto_ripte_si_ipc"].round(2),
        "Variación % respecto mes anterior RIPTE": df["percentage_of_variation"].round(2),
        "Variación % IPC Nacional General": df["monthly"].round(2),
        "Brecha %": df["brecha_pct"].round(2),
    })

    # Guardar CSV
    output_path.parent.mkdir(parents=True, exist_ok=True)
    df_output.to_csv(str(output_path), index=False, encoding="utf-8-sig")
    logger.info(f"CSV generado exitosamente: {output_path}")

    ultimo = df_output.iloc[-1]
    logger.info(
        f"Último período ({ultimo['Período']}): "
        f"RIPTE real=${ultimo['Monto en $ RIPTE']:,.2f} | "
        f"RIPTE simulado IPC=${ultimo['Monto en $ si los sueldos seguían al IPC']:,.2f} | "
        f"Brecha={ultimo['Brecha %']}%"
    )

    return df_output

def main() -> None:
    logger.info("=" * 60)
    logger.info("Inicio del análisis RIPTE vs IPC Nacional General (from DB)")
    logger.info("=" * 60)

    engine = get_engine()
    df_ripte, df_ipc = get_data(engine)
    
    df_result = build_analysis_csv(df_ripte, df_ipc, OUTPUT_CSV)

    print("\n" + "=" * 80)
    print("PREVIEW (primeros y últimos 5 registros)")
    print("=" * 80)
    print(df_result.head(5).to_string(index=False))
    print("...")
    print(df_result.tail(5).to_string(index=False))
    print(f"\nArchivo guardado en: {OUTPUT_CSV}")

if __name__ == "__main__":
    main()
