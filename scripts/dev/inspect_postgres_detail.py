"""
Script para profundizar en el detalle funcional y técnico de la BD PostgreSQL.
"""
import os
import json
from dotenv import load_dotenv
from sqlalchemy import create_engine, text

def analyze():
    load_dotenv()
    user = os.getenv("POSTGRE_USER", "postgres")
    pwd = os.getenv("POSTGRE_PASSWORD", "postgres")
    host = os.getenv("POSTGRE_HOST", "localhost")
    port = os.getenv("POSTGRE_PORT", "5432")
    db = os.getenv("POSTGRE_DB", "postgres")
    
    conn_str = f"postgresql+psycopg2://{user}:{pwd}@{host}:{port}/{db}"
    engine = create_engine(conn_str)
    
    with engine.connect() as conn:
        print("=== CONSTRAINTS & INDEXES ===")
        indexes = conn.execute(text("""
            SELECT
                tc.table_schema, 
                tc.table_name, 
                tc.constraint_name, 
                tc.constraint_type,
                kcu.column_name
            FROM information_schema.table_constraints tc
            LEFT JOIN information_schema.key_column_usage kcu
              ON tc.constraint_name = kcu.constraint_name
              AND tc.table_schema = kcu.table_schema
            WHERE tc.table_schema IN ('argentina_economy', 'raw_fundamentals', 'earnings')
            ORDER BY tc.table_schema, tc.table_name, tc.constraint_name;
        """)).fetchall()
        for idx in indexes:
            print(f"  {idx[0]}.{idx[1]}: {idx[2]} ({idx[3]}) -> {idx[4]}")

        print("\n=== DETALLE POR TABLA ===")
        
        # 1. earnings.historical_prices
        print("\n--- earnings.historical_prices ---")
        srcs = conn.execute(text("SELECT source, count(*), min(date), max(date), count(DISTINCT ticker) FROM earnings.historical_prices GROUP BY source;")).fetchall()
        for s in srcs:
            print(f"  Source: {s[0]} | Filas: {s[1]} | Fechas: {s[2]} a {s[3]} | Tickers distintos: {s[4]}")
        top_tickers = conn.execute(text("SELECT ticker, count(*), min(date), max(date) FROM earnings.historical_prices GROUP BY ticker ORDER BY count(*) DESC LIMIT 5;")).fetchall()
        print("  Top 5 tickers con más historial:")
        for t in top_tickers:
            print(f"    {t[0]}: {t[1]} ruedas ({t[2]} a {t[3]})")
            
        # 2. earnings.ccl_mep
        print("\n--- earnings.ccl_mep ---")
        ccl_info = conn.execute(text("""
            SELECT count(*), min(date), max(date), 
                   count(ccl) as count_ccl, count(mep) as count_mep,
                   min(ccl), max(ccl), min(mep), max(mep)
            FROM earnings.ccl_mep;
        """)).fetchone()
        print(f"  Total filas: {ccl_info[0]} | Rango: {ccl_info[1]} a {ccl_info[2]}")
        print(f"  CCL: min={ccl_info[5]}, max={ccl_info[6]} | MEP: min={ccl_info[7]}, max={ccl_info[8]}")
        
        # 3. earnings.ratios_cedears
        print("\n--- earnings.ratios_cedears ---")
        ratios_info = conn.execute(text("""
            SELECT partition_date, count(*), count(DISTINCT ticker)
            FROM earnings.ratios_cedears
            GROUP BY partition_date;
        """)).fetchall()
        for r in ratios_info:
            print(f"  partition_date: {r[0]} | Cantidad ratios: {r[1]} | Tickers: {r[2]}")
        sample_ratios = conn.execute(text("SELECT ticker, ratio FROM earnings.ratios_cedears ORDER BY ticker LIMIT 5;")).fetchall()
        print(f"  Ejemplos ratios: {sample_ratios}")

        # 4. raw_fundamentals.yfinance_cache
        print("\n--- raw_fundamentals.yfinance_cache ---")
        yf_cache = conn.execute(text("""
            SELECT statement_type, count(*), count(DISTINCT ticker), count(DISTINCT metric), min(as_of_date), max(as_of_date)
            FROM raw_fundamentals.yfinance_cache
            GROUP BY statement_type;
        """)).fetchall()
        for y in yf_cache:
            print(f"  statement_type: {y[0]} | Filas: {y[1]} | Tickers: {y[2]} | Métricas: {y[3]} | as_of_date: {y[4]} a {y[5]}")

        # 5. argentina_economy.ipc_monthly_year_on_year
        print("\n--- argentina_economy.ipc_monthly_year_on_year ---")
        ipc = conn.execute(text("""
            SELECT min(date), max(date), count(*)
            FROM argentina_economy.ipc_monthly_year_on_year;
        """)).fetchone()
        last_ipc = conn.execute(text("SELECT date, monthly, year_to_year FROM argentina_economy.ipc_monthly_year_on_year ORDER BY date DESC LIMIT 3;")).fetchall()
        print(f"  Rango: {ipc[0]} a {ipc[1]} ({ipc[2]} registros)")
        print(f"  Últimos registros: {last_ipc}")

        # 6. argentina_economy.ripte
        print("\n--- argentina_economy.ripte ---")
        ripte = conn.execute(text("""
            SELECT min(date), max(date), count(*)
            FROM argentina_economy.ripte;
        """)).fetchone()
        last_ripte = conn.execute(text("SELECT date, amount_ripte, percentage_of_variation FROM argentina_economy.ripte ORDER BY date DESC LIMIT 3;")).fetchall()
        print(f"  Rango: {ripte[0]} a {ripte[1]} ({ripte[2]} registros)")
        print(f"  Últimos registros: {last_ripte}")

        # Verificar si hay triggers, funciones o vistas
        print("\n=== OBJETOS ADICIONALES (Vistas, Rutinas, Extensiones) ===")
        views = conn.execute(text("SELECT table_schema, table_name FROM information_schema.views WHERE table_schema NOT IN ('pg_catalog', 'information_schema');")).fetchall()
        print(f"  Vistas: {views}")
        exts = conn.execute(text("SELECT extname, extversion FROM pg_extension;")).fetchall()
        print(f"  Extensiones: {exts}")

if __name__ == '__main__':
    analyze()
