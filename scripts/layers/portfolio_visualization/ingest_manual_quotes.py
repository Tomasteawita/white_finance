import os
import glob
import logging
import pandas as pd
from sqlalchemy import create_engine, text
from dotenv import load_dotenv

logging.basicConfig(level=logging.INFO, format="%(asctime)s - [%(levelname)s] - %(message)s")
logger = logging.getLogger("ManualIngest")

def get_engine():
    load_dotenv(r'c:\Users\tomas\white_finance\.env')
    user = os.getenv("POSTGRE_USER", "postgres")
    pwd = os.getenv("POSTGRE_PASSWORD", "postgres")
    host = os.getenv("POSTGRE_HOST", "localhost")
    port = os.getenv("POSTGRE_PORT", "5432")
    db = os.getenv("POSTGRE_DB", "postgres")
    conn_str = f"postgresql+psycopg2://{user}:{pwd}@{host}:{port}/{db}"
    return create_engine(conn_str)

def main():
    folder = r"c:\Users\tomas\white_finance\data\analytics\cotizaciones"
    engine = get_engine()
    
    files = glob.glob(os.path.join(folder, "*.*"))
    
    for f in files:
        ext = os.path.splitext(f)[1].lower()
        base_name = os.path.basename(f)
        
        if ext == ".csv":
            logger.info(f"Procesando CSV (Rava): {base_name}")
            df = pd.read_csv(f)
            
            # Format: especie,apertura,maximo,minimo,ultimo,cierre,fecha,volumen,timestamp
            # Extract ticker from the first part of 'especie' (e.g. AL30-0002-C... -> AL30)
            ticker_col = df['especie'].iloc[0] if 'especie' in df.columns else base_name.split('-')[0]
            ticker = ticker_col.split('-')[0].upper()
            
            normalized_df = pd.DataFrame()
            normalized_df['date'] = pd.to_datetime(df['fecha']).dt.date
            normalized_df['open'] = df['apertura']
            normalized_df['high'] = df['maximo']
            normalized_df['low'] = df['minimo']
            normalized_df['close'] = df['cierre']
            normalized_df['adj_close'] = df['cierre']
            normalized_df['volume'] = df['volumen']
            normalized_df['ticker'] = ticker
            normalized_df['source'] = 'Rava'
            
        elif ext in [".xlsx", ".xls"]:
            logger.info(f"Procesando Excel (BYMA): {base_name}")
            df = pd.read_excel(f)
            
            # Autodetect columns by printing for safety, but try standard BYMA mapping
            logger.info(f"Columnas detectadas en BYMA: {list(df.columns)}")
            
            if "SNSBO" in base_name.upper():
                ticker = "SNSBO"
            elif "LK01Q" in base_name.upper():
                ticker = "LK01Q"
            else:
                ticker = base_name.split('_')[2].upper() if len(base_name.split('_')) > 2 else "UNKNOWN"
            
            normalized_df = pd.DataFrame()
            # Busco las columnas más probables en BYMA
            date_col = next((c for c in df.columns if 'fecha' in c.lower() or 'date' in c.lower()), None)
            open_col = next((c for c in df.columns if 'apertura' in c.lower() or 'open' in c.lower()), None)
            high_col = next((c for c in df.columns if 'max' in c.lower() or 'high' in c.lower()), None)
            low_col = next((c for c in df.columns if 'min' in c.lower() or 'low' in c.lower()), None)
            close_col = next((c for c in df.columns if 'cierre' in c.lower() or 'close' in c.lower() or 'precio' in c.lower()), None)
            vol_col = next((c for c in df.columns if 'vol' in c.lower() or 'monto' in c.lower()), None)
            
            if not date_col or not close_col:
                logger.error(f"Faltan columnas clave en Excel. Abortando {base_name}")
                continue
                
            normalized_df['date'] = pd.to_datetime(df[date_col]).dt.date
            normalized_df['open'] = df[open_col] if open_col else df[close_col]
            normalized_df['high'] = df[high_col] if high_col else df[close_col]
            normalized_df['low'] = df[low_col] if low_col else df[close_col]
            normalized_df['close'] = df[close_col]
            normalized_df['adj_close'] = df[close_col]
            normalized_df['volume'] = df[vol_col] if vol_col else 0
            normalized_df['ticker'] = ticker
            normalized_df['source'] = 'Cohen'
            
        else:
            continue
            
        # Limpieza de nulos
        normalized_df.dropna(subset=['date', 'close'], inplace=True)
        
        insert_count = 0
        
        # Primero borramos
        with engine.begin() as conn:
            conn.execute(text("DELETE FROM earnings.historical_prices WHERE ticker = :t AND source = :s"), {"t": ticker, "s": normalized_df['source'].iloc[0]})
            
        # Lugeo insertamos fila a fila con commits individuales
        for _, row in normalized_df.iterrows():
            with engine.connect() as conn:
                try:
                    vol = 0
                    if pd.notna(row['volume']):
                        vol = int(float(row['volume']))
                        
                    conn.execute(text('''
                        INSERT INTO earnings.historical_prices (ticker, date, open, high, low, close, volume, source)
                        VALUES (:tk, :dt, :op, :hi, :lo, :cl, :vl, :sr)
                        ON CONFLICT (ticker, date) DO UPDATE SET
                            open = EXCLUDED.open,
                            high = EXCLUDED.high,
                            low = EXCLUDED.low,
                            close = EXCLUDED.close,
                            volume = EXCLUDED.volume,
                            source = EXCLUDED.source
                    '''), {
                        "tk": row['ticker'], "dt": row['date'], "op": float(row['open']),
                        "hi": float(row['high']), "lo": float(row['low']), "cl": float(row['close']),
                        "vl": vol, "sr": row['source']
                    })
                    conn.commit()
                    insert_count += 1
                except Exception as sub_e:
                    logger.error(f"Fila omitida ({row['date']}): {sub_e}")
                    
        logger.info(f"✅ {insert_count} registros insertados en Postgres para {ticker}.")

if __name__ == "__main__":
    main()
