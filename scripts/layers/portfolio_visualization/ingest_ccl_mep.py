import os
import yfinance as yf
import pandas as pd
from datetime import datetime
from sqlalchemy import create_engine, text
from dotenv import load_dotenv
import logging

logging.basicConfig(level=logging.INFO, format="%(asctime)s - [%(levelname)s] - %(message)s")
logger = logging.getLogger("TCExtractor")

def get_engine():
    load_dotenv(r'c:\Users\tomas\white_finance\.env')
    user = os.getenv("POSTGRE_USER", "postgres")
    pwd = os.getenv("POSTGRE_PASSWORD", "postgres")
    host = os.getenv("POSTGRE_HOST", "localhost")
    port = os.getenv("POSTGRE_PORT", "5432")
    db = os.getenv("POSTGRE_DB", "postgres")
    conn_str = f"postgresql+psycopg2://{user}:{pwd}@{host}:{port}/{db}"
    return create_engine(conn_str)

def create_table_if_not_exists(engine):
    ddl = """
    CREATE TABLE IF NOT EXISTS earnings.ccl_mep (
        date DATE PRIMARY KEY,
        ccl NUMERIC,
        mep NUMERIC
    );
    """
    with engine.begin() as conn:
        conn.execute(text(ddl))
        logger.info("Tabla earnings.ccl_mep validada.")

def get_ccl_mep_data():
    start_date = "2020-01-01" # Historial amplio compatible con las demas especies
    end_date = datetime.today().strftime('%Y-%m-%d')
    
    logger.info("Descargando GGAL (ADR en NYSE)...")
    ggal_adr = yf.download("GGAL", start=start_date, end=end_date)
    
    logger.info("Descargando GGAL.BA (Acción en ARS)...")
    ggal_ars = yf.download("GGAL.BA", start=start_date, end=end_date)
    
    logger.info("Descargando GGALD.BA (Acción MEP en USD)...")
    ggal_mep = yf.download("GGALD.BA", start=start_date, end=end_date)
    
    # Manejo de Multicolumnas de YFinance >= 0.2
    if isinstance(ggal_adr.columns, pd.MultiIndex):
        ggal_adr = ggal_adr['Close'].squeeze()
        ggal_ars = ggal_ars['Close'].squeeze()
        ggal_mep = ggal_mep['Close'].squeeze()
    else:
        ggal_adr = ggal_adr['Close']
        ggal_ars = ggal_ars['Close']
        ggal_mep = ggal_mep['Close']

    df_adr = pd.DataFrame({'Close_ADR': ggal_adr})
    df_ars = pd.DataFrame({'Close_ARS': ggal_ars})
    df_mep = pd.DataFrame({'Close_MEP': ggal_mep})

    # Join general por fecha
    df_combined = df_ars.join(df_adr, how='inner').join(df_mep, how='left')

    # Calcula CCL: Ratio GGAL.BA * 10 / GGAL_ADR
    df_combined['ccl'] = (df_combined['Close_ARS'] * 10) / df_combined['Close_ADR']
    
    # Calcula MEP: Ratio GGAL.BA / GGALD.BA
    df_combined['mep'] = df_combined['Close_ARS'] / df_combined['Close_MEP']
    
    # Fill backward or forward para MEP si GGALD no operó un dia especifico
    df_combined['mep'] = df_combined['mep'].ffill()
    
    df_final = df_combined[['ccl', 'mep']].dropna(subset=['ccl']).reset_index()
    df_final.rename(columns={'Date': 'date'}, inplace=True)
    df_final['date'] = pd.to_datetime(df_final['date']).dt.date
    
    return df_final

def ingest_to_db(df, engine):
    logger.info(f"Iniciando inserción de {len(df)} registros de tipo de cambio...")
    insert_count = 0
    with engine.begin() as conn:
        for _, row in df.iterrows():
            try:
                mep_val = float(row['mep']) if pd.notna(row['mep']) else None
                ccl_val = float(row['ccl']) if pd.notna(row['ccl']) else None
                
                conn.execute(text("""
                    INSERT INTO earnings.ccl_mep (date, ccl, mep)
                    VALUES (:dt, :ccl, :mep)
                    ON CONFLICT (date) DO UPDATE SET
                        ccl = EXCLUDED.ccl,
                        mep = EXCLUDED.mep
                """), {"dt": row['date'], "ccl": ccl_val, "mep": mep_val})
                insert_count += 1
            except Exception as e:
                logger.error(f"Fallo en fecha {row['date']}: {e}")
                
    logger.info(f"✅ Se actualizaron/insertaron {insert_count} cotizaciones (CCL/MEP) exitosamente.")

def main():
    engine = get_engine()
    create_table_if_not_exists(engine)
    tc_df = get_ccl_mep_data()
    ingest_to_db(tc_df, engine)

if __name__ == "__main__":
    main()
