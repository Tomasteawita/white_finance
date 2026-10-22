"""
Script para analizar en detalle la tabla earnings.ratios_cedears.
"""
import os
from dotenv import load_dotenv
from sqlalchemy import create_engine, text

def main():
    load_dotenv()
    user = os.getenv("POSTGRE_USER", "postgres")
    pwd = os.getenv("POSTGRE_PASSWORD", "postgres")
    host = os.getenv("POSTGRE_HOST", "localhost")
    port = os.getenv("POSTGRE_PORT", "5432")
    db = os.getenv("POSTGRE_DB", "postgres")
    engine = create_engine(f"postgresql+psycopg2://{user}:{pwd}@{host}:{port}/{db}")
    
    with engine.connect() as conn:
        # 1. Particionamiento físico
        part_info = conn.execute(text("""
            SELECT c.relkind, 
                   pg_get_partkeydef(c.oid) as partkey,
                   (SELECT count(*) FROM pg_inherits WHERE inhparent = c.oid) as subpartitions
            FROM pg_class c
            JOIN pg_namespace n ON n.oid = c.relnamespace
            WHERE n.nspname = 'earnings' AND c.relname = 'ratios_cedears';
        """)).fetchone()
        
        print("=== PARTICIONAMIENTO FÍSICO (MOTOR POSTGRESQL) ===")
        print(f"Tipo (relkind): {part_info[0]} (r = regular table, p = partitioned table)")
        print(f"Partkey DDL: {part_info[1]}")
        print(f"Subparticiones físicas: {part_info[2]}")
        
        # 2. Particionamiento lógico (columna partition_date)
        print("\n=== PARTICIONAMIENTO LÓGICO (COLUMNA partition_date) ===")
        partitions = conn.execute(text("""
            SELECT 
                partition_date, 
                count(*) as total_filas,
                count(DISTINCT ticker) as total_especies,
                count(DISTINCT TRIM(ticker)) as total_especies_trim
            FROM earnings.ratios_cedears
            GROUP BY partition_date
            ORDER BY partition_date;
        """)).fetchall()
        for p in partitions:
            print(f"Fecha partición: '{p[0]}' | Filas: {p[1]} | Especies únicas: {p[2]} (Trimmed: {p[3]})")
            
        # 3. Muestra de especies
        tickers = conn.execute(text("SELECT ticker, ratio FROM earnings.ratios_cedears ORDER BY ticker LIMIT 15;")).fetchall()
        print("\nPrimeras 15 especies registradas:")
        for t in tickers:
            print(f"  {repr(t[0])}: ratio = {t[1]}")

        # 4. Estadísticas generales
        stats = conn.execute(text("""
            SELECT 
                count(*) as total_filas,
                count(DISTINCT ticker) as total_especies,
                min(ratio) as min_ratio,
                max(ratio) as max_ratio,
                avg(ratio) as avg_ratio
            FROM earnings.ratios_cedears;
        """)).fetchone()
        print("\nEstadísticas generales:")
        print(f"  Total filas: {stats[0]}")
        print(f"  Especies distintas: {stats[1]}")
        print(f"  Ratio mínimo: {stats[2]}")
        print(f"  Ratio máximo: {stats[3]}")
        print(f"  Ratio promedio: {stats[4]:.2f}")

if __name__ == '__main__':
    main()
