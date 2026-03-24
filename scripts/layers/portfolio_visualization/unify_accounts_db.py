import os
import pandas as pd
from dotenv import load_dotenv

class UnifiedAccountPricer:
    """
    Clase que toma las 3 cuentas corrientes crudas (Pesos, MEP, Cable)
    y unifica sus registros en Dolares MEP consultando el tipo de cambio
    institucional directamente desde PostgreSQL (earnings.ccl_mep).
    """
    
    def __init__(self, csv_pesos, csv_mep, csv_ccl):
        self.csv_pesos = csv_pesos
        self.csv_mep = csv_mep
        self.csv_ccl = csv_ccl
        self.columns_needed = [
            'Liquida', 'Operado', 'Comprobante', 'Numero', 
            'Cantidad', 'Especie', 'Precio', 'Importe', 'Saldo', 'Referencia', 'Origen'
        ]
        
    def _get_engine(self):
        from sqlalchemy import create_engine, text
        # Localizamos el root project dinamico o asumimos env config.
        env_path = r'c:\Users\tomas\white_finance\.env'
        if os.path.exists(env_path):
            load_dotenv(env_path)
            
        user = os.getenv("POSTGRE_USER", "postgres")
        pwd = os.getenv("POSTGRE_PASSWORD", "postgres")
        host = os.getenv("POSTGRE_HOST", "localhost")
        port = os.getenv("POSTGRE_PORT", "5432")
        db = os.getenv("POSTGRE_DB", "postgres")
        conn_str = f"postgresql+psycopg2://{user}:{pwd}@{host}:{port}/{db}"
        return create_engine(conn_str)
        
    def _fetch_fx_rates(self):
        engine = self._get_engine()
        query = "SELECT date, ccl, mep FROM earnings.ccl_mep"
        df_fx = pd.read_sql(query, engine)
        df_fx['date'] = pd.to_datetime(df_fx['date'])
        return df_fx

    def get_unified_accounts_usd(self):
        # 1. Cargar las 3 Cuentas originales
        print("Cargando CSVs originales...")
        df_pesos = pd.read_csv(self.csv_pesos)
        df_mep = pd.read_csv(self.csv_mep)
        df_ccl = pd.read_csv(self.csv_ccl)
        
        # 2. Agregar origen
        df_pesos['Origen'] = 'ARS'
        df_mep['Origen'] = 'USD MEP'
        df_ccl['Origen'] = 'USD CCL'
        
        # 3. Traer cotizaciones histÃ³ricas de BBDD
        print("Obteniendo cotizaciones históricas CCL/MEP desde Postgres...")
        df_fx = self._fetch_fx_rates()
        
        # Parse Dates
        df_pesos['Operado'] = pd.to_datetime(df_pesos['Operado'], dayfirst=True, errors='coerce')
        if df_pesos['Operado'].isna().sum() > 0:
            df_pesos['Operado'] = pd.to_datetime(df_pesos['Operado'], format='%Y-%m-%d', errors='coerce')
            
        # 4. Merger pesos con MEP para dolarizar la cuenta en ARS
        print("Dolarizando cuenta ARS a USD MEP...")
        df_pesos = pd.merge(df_pesos, df_fx, left_on='Operado', right_on='date', how='left')
        
        # Parche para Fines de Semana u Operaciones Feriadas (arrastra el dÃ³lar del Ãºltimo dÃ­a hÃ¡bil)
        df_pesos = df_pesos.sort_values('Operado')
        df_pesos['mep'] = df_pesos['mep'].ffill().bfill()
        
        # Calculamos importe y saldo en USD MEP
        df_pesos['Importe'] = df_pesos['Importe'] / df_pesos['mep']
        df_pesos['Saldo'] = df_pesos['Saldo'] / df_pesos['mep']
        
        # 5. Formatear y Unir
        df_pesos = df_pesos[self.columns_needed]
        
        # Los dólares crudos ya están en dólares, no hay que tocarlos.
        # Solo verificamos que contengan los nombres oficiales
        for col in self.columns_needed:
            if col not in df_mep.columns: df_mep[col] = None
            if col not in df_ccl.columns: df_ccl[col] = None
                
        df_mep = df_mep[self.columns_needed]
        df_ccl = df_ccl[self.columns_needed]
        
        print("Unificando y ordenando las tres cuentas en USD...")
        df_unified = pd.concat([df_pesos, df_mep, df_ccl], ignore_index=True)
        
        df_unified['Operado'] = pd.to_datetime(df_unified['Operado'])
        
        # Eliminamos duplicados reales por clave compuesta (Numero + Especie) para no pisar Block Trades
        df_unified = df_unified.sort_values(by=['Numero', 'Especie', 'Operado'], na_position='last')
        df_unified = df_unified.drop_duplicates(subset=['Numero', 'Especie'], keep='first')
        
        # Restaurar orden cronológico
        df_unified = df_unified.sort_values(by=['Liquida', 'Operado', 'Numero']).reset_index(drop=True)
        
        print(f"Cuentas Dolarizadas Correctamente! Registros finales univocos: {len(df_unified)}")
        return df_unified
