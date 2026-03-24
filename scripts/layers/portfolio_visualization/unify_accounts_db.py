import os
import pandas as pd
from dotenv import load_dotenv

class UnifiedAccountPricer:
    """
    Clase que toma las 3 cuentas corrientes crudas (Pesos, MEP, Cable)
    y unifica sus registros en Dolares CCL (Cable) consultando el tipo de cambio
    institucional directamente desde PostgreSQL (earnings.ccl_mep).
    Además, transforma los CEDEARs a sus cantidades subyacentes equivalentes en EE.UU.
    y normaliza los tickers eliminando sufijos locales.
    """
    
    def __init__(self, csv_pesos, csv_mep, csv_ccl):
        self.csv_pesos = csv_pesos
        self.csv_mep = csv_mep
        self.csv_ccl = csv_ccl
        self.columns_needed = [
            'Liquida', 'Operado', 'Comprobante', 'Numero', 
            'Cantidad', 'Especie', 'Precio', 'Importe', 'Saldo', 'Referencia', 'Origen'
        ]
        
        # Diccionario unificado de Ratios
        self.ratios_cedear = {
            'KO': 5.0, 'SPY': 20.0, 'QQQ': 20.0, 'AAPL': 10.0,
            'GOOGL': 58.0, 'MSFT': 30.0, 'TSLA': 15.0, 'MELI': 120.0,
            'LLY': 56.0, 'META': 24.0, 'VIST': 3.0, 'AMZN': 144.0,
            'NVDA': 24.0, 'NFLX': 60.0, 'TLT': 1.0, 'SH': 8.0,
            'ARGT': 1.0, 'XLP': 1.0, 'SHY': 1.0, 'ADBE': 44.0,
            'ARKK': 10.0, 'ASML': 146.0, 'BBD': 1.0, 'BIOX': 1.0,
            'COIN': 27.0, 'ERIC': 2.0, 'HMY': 1.0, 'LAR': 1.0,
            'PAAS': 3.0, 'PSQ': 8.0, 'SAN': 0.25, 'UNH': 33.0, 'VALE': 2.0
        }
        
    def _get_engine(self):
        from sqlalchemy import create_engine
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

    def _parse_dates(self, df):
        """Helper para parsear uniformemente las fechas en YYYY-MM-DD"""
        df['Operado'] = pd.to_datetime(df['Operado'], format='%Y-%m-%d', errors='coerce')
        df['Liquida'] = pd.to_datetime(df['Liquida'], format='%Y-%m-%d', errors='coerce')
        return df

    def _clean_tickers(self, df):
        """
        Elimina rigurosamente los sufijos .BA y .US al final de la cadena
        para normalizar la especie al mercado americano, sin afectar tickers
        como YPFD o BBD.
        """
        if 'Especie' in df.columns:
            df['Especie'] = df['Especie'].astype(str).str.replace(r'\.BA$', '', regex=True).str.replace(r'\.US$', '', regex=True)
            # Reemplazar los 'nan' stringificados por None
            df['Especie'] = df['Especie'].replace('nan', None)
        return df

    def get_unified_accounts_usd(self):
        print("Cargando CSVs originales...")
        df_pesos = pd.read_csv(self.csv_pesos)
        df_mep = pd.read_csv(self.csv_mep)
        df_ccl = pd.read_csv(self.csv_ccl)
        
        df_pesos['Origen'] = 'ARS'
        df_mep['Origen'] = 'USD MEP'
        df_ccl['Origen'] = 'USD CCL'
        
        print("Parseando fechas y limpiando sufijos de tickers...")
        # Limpieza de fechas y tickers desde el momento de carga
        df_pesos = self._clean_tickers(self._parse_dates(df_pesos))
        df_mep = self._clean_tickers(self._parse_dates(df_mep))
        df_ccl = self._clean_tickers(self._parse_dates(df_ccl))
        
        print("Obteniendo cotizaciones históricas CCL/MEP desde Postgres...")
        df_fx = self._fetch_fx_rates()
            
        # ====================================================================
        # 1. Transformación de Cuenta en ARS a USD CCL
        # ====================================================================
        print("Convirtiendo cuenta ARS a USD CCL y aplicando ratios de CEDEARs...")
        df_pesos = pd.merge(df_pesos, df_fx, left_on='Operado', right_on='date', how='left')
        df_pesos = df_pesos.sort_values('Operado')
        df_pesos['ccl'] = df_pesos['ccl'].ffill().bfill()
        
        # Pasaje de métricas a CCL
        df_pesos['Precio'] = df_pesos['Precio'] / df_pesos['ccl']
        df_pesos['Importe'] = df_pesos['Importe'] / df_pesos['ccl']
        df_pesos['Saldo'] = df_pesos['Saldo'] / df_pesos['ccl']
        
        # Mapear ratios (La especie ya está limpia)
        ratios_mapeados = df_pesos['Especie'].map(self.ratios_cedear).fillna(1.0)
        
        # Aplicar el ajuste de CEDEARs
        df_pesos['Cantidad'] = df_pesos['Cantidad'] / ratios_mapeados
        df_pesos['Precio'] = df_pesos['Precio'] * ratios_mapeados
        
        df_pesos = df_pesos[self.columns_needed]

        # ====================================================================
        # 2. Transformación de Cuenta en USD MEP a USD CCL
        # ====================================================================
        print("Convirtiendo cuenta USD MEP a USD CCL...")
        df_mep = pd.merge(df_mep, df_fx, left_on='Operado', right_on='date', how='left')
        df_mep = df_mep.sort_values('Operado')
        df_mep['mep'] = df_mep['mep'].ffill().bfill()
        df_mep['ccl'] = df_mep['ccl'].ffill().bfill()
        
        # Para pasar MEP a CCL: Multiplicamos por la brecha (mep / ccl)
        factor_brecha = df_mep['mep'] / df_mep['ccl']
        df_mep['Precio'] = df_mep['Precio'] * factor_brecha
        df_mep['Importe'] = df_mep['Importe'] * factor_brecha
        df_mep['Saldo'] = df_mep['Saldo'] * factor_brecha
        
        df_mep = df_mep[self.columns_needed]

        # ====================================================================
        # 3. Integración de Cuenta USD CCL
        # ====================================================================
        # Ya está en la moneda correcta, sólo verificamos integridad de columnas
        for col in self.columns_needed:
            if col not in df_ccl.columns: 
                df_ccl[col] = None
        df_ccl = df_ccl[self.columns_needed]
        
        print("Unificando y ordenando las tres cuentas bajo el estándar USD CCL...")
        df_unified = pd.concat([df_pesos, df_mep, df_ccl], ignore_index=True)
        
        # Eliminamos duplicados reales
        df_unified = df_unified.sort_values(by=['Numero', 'Especie', 'Operado'], na_position='last')
        df_unified = df_unified.drop_duplicates(subset=['Numero', 'Especie'], keep='first')
        
        # Restaurar orden cronológico estricto (Liquida -> Operado)
        df_unified = df_unified.sort_values(by=['Liquida', 'Operado', 'Numero']).reset_index(drop=True)
        
        # Volvemos a formatear las fechas en string ISO para guardarlo limpiamente en el CSV
        df_unified['Operado'] = df_unified['Operado'].dt.strftime('%Y-%m-%d')
        df_unified['Liquida'] = df_unified['Liquida'].dt.strftime('%Y-%m-%d')
        
        print(f"Dataset Dolarizado en CCL Exitosamente! Registros finales unívocos: {len(df_unified)}")
        return df_unified