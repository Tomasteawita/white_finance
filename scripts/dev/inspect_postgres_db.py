"""
Script de inspección profunda de la base de datos PostgreSQL.
Analiza esquemas, tablas, particiones, rangos temporales, volumen y estructura funcional.
"""
import os
import json
from dotenv import load_dotenv
from sqlalchemy import create_engine, text

def inspect_db():
    load_dotenv()
    user = os.getenv("POSTGRE_USER", "postgres")
    pwd = os.getenv("POSTGRE_PASSWORD", "postgres")
    host = os.getenv("POSTGRE_HOST", "localhost")
    port = os.getenv("POSTGRE_PORT", "5432")
    db = os.getenv("POSTGRE_DB", "postgres")
    
    conn_str = f"postgresql+psycopg2://{user}:{pwd}@{host}:{port}/{db}"
    print(f"Conectando a {user}@{host}:{port}/{db}...")
    engine = create_engine(conn_str)
    
    with engine.connect() as conn:
        # 1. Databases en el cluster
        dbs = conn.execute(text("SELECT datname FROM pg_database WHERE datistemplate = false;")).fetchall()
        print("\n=== BASES DE DATOS EN EL CLUSTER ===")
        for d in dbs:
            print(f" - {d[0]}")
            
        # 2. Esquemas de usuario
        schemas_res = conn.execute(text("""
            SELECT schema_name 
            FROM information_schema.schemata 
            WHERE schema_name NOT IN ('pg_catalog', 'information_schema') 
              AND schema_name NOT LIKE 'pg_%';
        """)).fetchall()
        schemas = [s[0] for s in schemas_res]
        print(f"\n=== ESQUEMAS EN '{db}' ===")
        print(schemas)
        
        # 3. Tablas y Vistas por esquema
        tables_res = conn.execute(text("""
            SELECT table_schema, table_name, table_type
            FROM information_schema.tables
            WHERE table_schema NOT IN ('pg_catalog', 'information_schema')
              AND table_schema NOT LIKE 'pg_%'
            ORDER BY table_schema, table_name;
        """)).fetchall()
        
        print(f"\n=== TABLAS Y VISTAS ({len(tables_res)}) ===")
        for t in tables_res:
            schema, name, t_type = t
            print(f"\n[{schema}.{name}] ({t_type})")
            
            # Columnas
            cols = conn.execute(text(f"""
                SELECT column_name, data_type, is_nullable
                FROM information_schema.columns
                WHERE table_schema = '{schema}' AND table_name = '{name}'
                ORDER BY ordinal_position;
            """)).fetchall()
            col_strs = [f"{c[0]} ({c[1]})" for c in cols]
            print(f"  Columnas: {', '.join(col_strs)}")
            
            # Cantidad de filas
            try:
                cnt = conn.execute(text(f'SELECT count(*) FROM "{schema}"."{name}"')).scalar()
                print(f"  Filas: {cnt}")
            except Exception as e:
                print(f"  Filas: Error ({e})")
                cnt = 0
                
            # Verificar si tiene particionamiento
            part_info = conn.execute(text(f"""
                SELECT 
                    c.relkind,
                    pg_get_expr(c.relpartbound, c.oid) as partition_bound,
                    (SELECT count(*) FROM pg_inherits WHERE inhparent = c.oid) as child_partitions
                FROM pg_class c
                JOIN pg_namespace n ON n.oid = c.relnamespace
                WHERE n.nspname = '{schema}' AND c.relname = '{name}';
            """)).fetchone()
            if part_info:
                relkind, partbound, child_cnt = part_info
                # 'p' = partitioned table, 'r' = normal table
                is_part = "Particionada (padre)" if relkind == 'p' else ("Partición hija" if partbound else "Ordinaria (no particionada)")
                print(f"  Tipo Físico / Partición: {is_part}")
                if child_cnt and child_cnt > 0:
                    print(f"  Subparticiones hijas: {child_cnt}")
                if partbound:
                    print(f"  Límite de partición: {partbound}")
                    
            # Si hay columnas de fecha/tiempo, inspeccionar rango temporal
            date_cols = [c[0] for c in cols if 'date' in c[1].lower() or 'time' in c[1].lower()]
            if date_cols and cnt > 0:
                for dc in date_cols:
                    try:
                        min_max = conn.execute(text(f'SELECT min("{dc}"), max("{dc}"), count(DISTINCT "{dc}") FROM "{schema}"."{name}"')).fetchone()
                        print(f"  Rango temporal en '{dc}': min={min_max[0]}, max={min_max[1]}, fechas_distintas={min_max[2]}")
                    except Exception as e:
                        print(f"  Rango temporal en '{dc}': Error ({e})")

            # Muestra de datos (primeras 2 filas)
            if cnt > 0:
                try:
                    sample = conn.execute(text(f'SELECT * FROM "{schema}"."{name}" LIMIT 2')).fetchall()
                    print(f"  Muestra (2 filas):")
                    for s in sample:
                        print(f"    {dict(zip([c[0] for c in cols], s))}")
                except Exception as e:
                    print(f"  Muestra: Error ({e})")

if __name__ == '__main__':
    inspect_db()
