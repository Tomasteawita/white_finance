"""
Script de re-extracción masiva para rellenar huecos históricos en PostgreSQL.
Lee todas las especies operadas en la cuenta corriente, detecta cuáles tienen
datos insuficientes en la BD, y descarga el historial de YFinance desde 2022.

Se guarda en: scripts/dev/ (uso interno)
"""
import pandas as pd
import yfinance as yf
from sqlalchemy import create_engine, text
import os
from dotenv import load_dotenv
from datetime import datetime

load_dotenv(r'c:\Users\tomas\white_finance\.env')
user = os.getenv('POSTGRE_USER', 'postgres')
pwd = os.getenv('POSTGRE_PASSWORD', 'postgres')
host = os.getenv('POSTGRE_HOST', 'localhost')
port = os.getenv('POSTGRE_PORT', '5432')
db = os.getenv('POSTGRE_DB', 'postgres')
engine = create_engine(f'postgresql+psycopg2://{user}:{pwd}@{host}:{port}/{db}')

# 1. Leer todas las operaciones
df_cc = pd.read_csv(r'c:\Users\tomas\white_finance\data\analytics\cuentas_unificadas_usd_sorted.csv')
df_cc['Operado'] = pd.to_datetime(df_cc['Operado'])

# Tickers locales (excluyendo FCIs, bonos y caucion)
fcis = ['ALGIIA', 'BMACTAA', 'BULL-IA', 'BULMAAA', 'RIGAHOR']
bonds = ['AE38', 'AL30', 'GD30', 'GD35', 'SNSBO', 'LK01Q', 'AL30D', 'GD30D']
skip_comprobantes = ['DIVIDENDOS', 'RENTA Y AMORTIZ', 'DEPOSITO', 'EXTRACCION', 'ACREDITACION']
cedears_usd = ['SPY', 'QQQ', 'AAPL', 'KO', 'TLT', 'SH', 'ARGT', 'XLP', 'SHY', 'ARKK',
               'GOOGL', 'MSFT', 'TSLA', 'MELI', 'LLY', 'META', 'VIST', 'AMZN', 'NVDA', 'NFLX']

valid_ops = df_cc[~df_cc['Comprobante'].isin(skip_comprobantes)].copy()
valid_ops['Especie_Base'] = valid_ops['Especie'].apply(
    lambda x: str(x).replace('.US', '').replace('.BA', '') if pd.notna(x) else x
)

# Solo activos locales de ByMA (los que probablemente tengan huecos)
especies_locales = [e for e in valid_ops['Especie_Base'].unique()
                    if e not in fcis and e not in bonds and e not in cedears_usd
                    and pd.notna(e) and str(e) not in ['nan', 'VARIAS']]

print(f"Especies locales a re-extraer: {especies_locales}")

# 2. Para cada especie, obtener la fecha de primer dato en BD
with engine.connect() as conn:
    result = conn.execute(text(
        "SELECT ticker, MIN(date) as first_date FROM earnings.historical_prices GROUP BY ticker"
    ))
    db_dates = {row[0]: row[1] for row in result}

# 3. Determinar qué especies necesitan backfill
TARGET_START = '2021-01-01'
needs_update = []
for esp in especies_locales:
    first_in_db = db_dates.get(esp)
    first_op = valid_ops[valid_ops['Especie_Base'] == esp]['Operado'].min()
    if first_in_db is None or pd.to_datetime(first_in_db) > first_op:
        needs_update.append((esp, first_op))
        print(f"  HUECO: {esp} | primer op: {first_op.date()} | primer dato BD: {first_in_db}")

print(f"\nTotal con huecos: {len(needs_update)}")

# 4. Re-extraer de YFinance e insertar
for esp, first_op in needs_update:
    ticker_yf = f"{esp}.BA"
    print(f"\n⬇️  Descargando {ticker_yf} desde {TARGET_START}...")
    try:
        hist = yf.download(ticker_yf, start=TARGET_START, progress=False, auto_adjust=True)
        if hist.empty:
            print(f"  ⚠️  Sin datos para {ticker_yf}")
            continue

        df = hist.reset_index()
        if isinstance(df.columns, pd.MultiIndex):
            df.columns = ['_'.join(c).strip('_') for c in df.columns]
        
        # Normalizar columnas
        col_map = {}
        for c in df.columns:
            cl = c.lower()
            if 'date' in cl: col_map[c] = 'date'
            elif 'close' in cl: col_map[c] = 'close'
            elif 'open' in cl: col_map[c] = 'open'
            elif 'high' in cl: col_map[c] = 'high'
            elif 'low' in cl: col_map[c] = 'low'
            elif 'volume' in cl: col_map[c] = 'volume'
        df.rename(columns=col_map, inplace=True)
        
        df['ticker'] = esp
        df['source'] = 'YFinance_backfill'
        df['date'] = pd.to_datetime(df['date']).dt.date
        
        df = df[['date', 'ticker', 'open', 'high', 'low', 'close', 'volume', 'source']].dropna(subset=['close'])
        
        # Upsert (INSERT ... ON CONFLICT UPDATE)
        inserted = 0
        with engine.begin() as conn:
            for _, row in df.iterrows():
                conn.execute(text("""
                    INSERT INTO earnings.historical_prices (date, ticker, open, high, low, close, volume, source)
                    VALUES (:date, :ticker, :open, :high, :low, :close, :volume, :source)
                    ON CONFLICT (date, ticker) DO UPDATE
                    SET close = EXCLUDED.close, source = EXCLUDED.source
                """), row.to_dict())
                inserted += 1
        
        print(f"  ✅  {esp}: {inserted} registros insertados/actualizados")
    except Exception as e:
        print(f"  ❌  Error con {esp}: {e}")

print("\n✅ Backfill de precios completado.")
