import pandas as pd
import numpy as np
from sqlalchemy import create_engine
import os
from dotenv import load_dotenv
from datetime import datetime

class VCPEngine:
    """
    Motor Mark-to-Market institucional. 
    Cruza el flujo temporal de cuentas unificadas contra PostgreSQL para determinar 
    el Valor Cuotaparte (VCP) aplicando la separación "Barbell" de los activos.
    """
    def __init__(self, csv_path: str):
        self.csv_path = csv_path
        self.df_cuentas = pd.read_csv(self.csv_path)
        self.df_cuentas['Operado'] = pd.to_datetime(self.df_cuentas['Operado'])
        
        # Estrategia Táctica
        self.bonds = ['AE38', 'AL30', 'GD30', 'GD35', 'SNSBO', 'LK01Q', 'AL30D', 'GD30D']
        self.safe_bonds = ['SNSBO', 'LK01Q'] # Activos cortos
        
        # Identificadores de FCIs Conservadores
        self.fcis = ['ALGIIA', 'BMACTAA', 'BULL-IA', 'BULMAAA', 'RIGAHOR']
        
        # Opcionalidad de CEDEARs (Versión 0 - Diccionario incompleto)
        self.ratios_cedear = {
            'KO': 5.0, 'SPY': 20.0, 'QQQ': 20.0, 'AAPL': 10.0,
            'GOOGL': 58.0, 'MSFT': 30.0, 'TSLA': 15.0, 'MELI': 60.0,
            'LLY': 60.0, 'META': 24.0, 'VIST': 3.0, 'AMZN': 144.0,
            'NVDA': 24.0, 'NFLX': 60.0, 'TLT': 1.0, 'SH': 1.0,
            'ARGT': 1.0, 'XLP': 1.0, 'SHY': 1.0, 'ADBE': 44.0,
            'ARKK': 10.0, 'ASML': 146.0, 'BBD': 1, 'BIOX': 1,
            'COIN': 27, 'ERIC': 2, 'HMY': 1, 'LAR': 1, 'LLY': 56,
            'MELI': 120, 'META': 24, 'MSFT': 30, 'PAAS': 3, 'PSQ': 8,
            'SAN': 0.25, 'SH': 8, 'TSLA': 15, 'UNH': 33, 'VALE': 2, 'VIST': 3
        }
        
    def _get_engine(self):
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

    def _is_fci(self, especie):
        esp_str = str(especie).upper()
        return any(f in esp_str for f in self.fcis) or "FCI" in esp_str

    def build_daily_vcp(self):
        # 1. Preparación y Filtro de Comprobantes Transaccionales
        valid_comprobantes = [
            'COMPRA EXTERIOR V', 'VENTA EXTERIOR V', 
            'COMPRA NORMAL', 'VENTA', 
            'COMPRA CAUCION CONTADO', 'VENTA CAUCION TERMINO',
            'LICITACION PRIVADA',
            'COMPRA PARIDAD', 'VENTA PARIDAD',
            'SUSCRIPCION FCI', 'RESCATE FCI'
        ]
        
        df_trades = self.df_cuentas[self.df_cuentas['Comprobante'].isin(valid_comprobantes) & 
                                    ~self.df_cuentas['Comprobante'].str.contains('TRADING')].copy()
                                    
        # Normalizar Especie retirando sufijos
        df_trades['Especie_Base'] = df_trades['Especie'].apply(lambda x: str(x).replace('.US', '').replace('.BA', '').replace('D', '') if pd.notna(x) else x)
        df_trades['Especie_Base'] = df_trades['Especie'].apply(lambda x: str(x).replace('.US', '').replace('.BA', '') if pd.notna(x) else x)
        
        # Ajuste histórico del script viejo para cantidades CEDEARs
        def adjust_qty(row):
            esp = str(row['Especie'])
            qty = abs(float(row['Cantidad'])) if pd.notna(row['Cantidad']) else 0.0
            if esp.endswith('.US'):
                base = esp.replace('.US', '')
                if base in self.ratios_cedear:
                    return qty * self.ratios_cedear[base]
            return qty

        df_trades['Cantidad_Ajustada'] = df_trades.apply(adjust_qty, axis=1)
        
        ventas_comprobantes = ['VENTA', 'VENTA EXTERIOR V', 'VENTA CAUCION TERMINO', 'VENTA PARIDAD', 'RESCATE FCI']
        df_trades['Signo_Cant'] = df_trades['Comprobante'].apply(lambda x: -1 if x in ventas_comprobantes else 1)
        df_trades['Flujo_Cant'] = df_trades['Cantidad_Ajustada'] * df_trades['Signo_Cant']
        df_trades['Flujo_Importe_USD'] = df_trades['Importe'].fillna(0.0)

        # 2. Reestructuración de Matriz de Tiempos
        min_date = df_trades['Operado'].min()
        max_date = datetime.today().date()
        date_range = pd.date_range(start=min_date, end=max_date)
        
        df_grid = pd.DataFrame({'Operado': date_range})
        especies = df_trades['Especie_Base'].unique()
        
        # Traer saldos de liquidez (Cash en Dólares) unificados
        df_saldos = self.df_cuentas[['Operado', 'Origen', 'Saldo']].copy()
        df_saldos['Operado'] = pd.to_datetime(df_saldos['Operado'])
        df_saldos = df_saldos.dropna(subset=['Saldo'])
        df_cash_pivot = df_saldos.groupby(['Operado', 'Origen'])['Saldo'].last().unstack().reset_index()
        df_cash_pivot['Cash_Total_USD'] = df_cash_pivot.drop(columns=['Operado']).sum(axis=1)
        
        # (En la Versión 0 no existía el bloque de cálculo de cauciones)
        
        # Merge de Cash con la Grilla
        df_vcp = pd.merge(df_grid, df_cash_pivot[['Operado', 'Cash_Total_USD']], on='Operado', how='left')
        df_vcp['Cash_Total_USD'] = df_vcp['Cash_Total_USD'].ffill()

        # 3. Traer Base de Datos Histórica (Precios PostgreSQL)
        engine = self._get_engine()
        query = "SELECT date, ticker, close FROM earnings.historical_prices"
        df_db = pd.read_sql(query, engine)
        df_db['date'] = pd.to_datetime(df_db['date'])
        
        df_prices = df_db.pivot_table(index='date', columns='ticker', values='close').reset_index()
        df_prices = pd.merge(df_grid, df_prices, left_on='Operado', right_on='date', how='left').drop(columns=['date'])
        
        query_fx = "SELECT date, mep FROM earnings.ccl_mep"
        df_fx = pd.read_sql(query_fx, engine)
        df_fx['date'] = pd.to_datetime(df_fx['date'])
        df_prices = pd.merge(df_prices, df_fx, left_on='Operado', right_on='date', how='left').drop(columns=['date'])
        
        # Extrapolar valuación (Versión 0 usaba bfill() causando anomalías)
        df_prices.sort_values('Operado', inplace=True)
        df_prices = df_prices.ffill().bfill()

        # 4. Cálculo Iterativo de Equities (Holdings) + Barbell Clusters
        val_safe = np.zeros(len(df_grid))
        val_growth = np.zeros(len(df_grid))
        
        known_cedears = ['SPY', 'QQQ', 'AAPL', 'KO', 'TLT', 'SH', 'ARGT', 'XLP', 'SHY']
        
        for esp in especies:
            df_esp = df_trades[df_trades['Especie_Base'] == esp].groupby('Operado').agg({'Flujo_Cant':'sum', 'Flujo_Importe_USD':'sum'}).reset_index()
            merged = pd.merge(df_grid, df_esp, on='Operado', how='left')
            merged['Flujo_Cant'] = merged['Flujo_Cant'].fillna(0)
            merged['Flujo_Importe_USD'] = merged['Flujo_Importe_USD'].fillna(0)
            merged['Holdings'] = merged['Flujo_Cant'].cumsum().clip(lower=0)
            
            if self._is_fci(esp):
                val_diaria = (-merged['Flujo_Importe_USD']).cumsum().clip(lower=0)
                val_safe += val_diaria.values
            else:
                ticker_db = esp
                if ticker_db in df_prices.columns:
                    serie_precios = df_prices[ticker_db].fillna(0)
                else:
                    serie_precios = np.zeros(len(df_grid))
                    
                if ticker_db in self.ratios_cedear:
                    # Lógica original: si no está en el dict, se asume ARS (Error con SH, TLT, etc)
                    ratio = self.ratios_cedear.get(ticker_db, 1.0)
                    precio_usd = serie_precios / ratio
                else:
                    factor_bono = 100.0 if any(b in ticker_db for b in self.bonds) else 1.0
                    mep_diario = df_prices['mep'].fillna(1000.0) 
                    precio_usd = (serie_precios / factor_bono) / mep_diario
                    
                val_diaria = merged['Holdings'] * precio_usd
                
                if any(s in ticker_db for s in self.safe_bonds):
                    val_safe += val_diaria.values
                else:
                    val_growth += val_diaria.values
                    
        # 5. Integración del VCP Final (Versión 0 sin Cauciones)
        df_vcp['Total_Safe_Valuation'] = val_safe + df_vcp['Cash_Total_USD'].fillna(0)
        df_vcp['Total_Growth_Valuation'] = val_growth
        df_vcp['Valor_Cuotaparte_USD'] = df_vcp['Total_Safe_Valuation'] + df_vcp['Total_Growth_Valuation']
        
        return df_vcp

def main():
    csv_path = r'c:\Users\tomas\white_finance\data\analytics\cuentas_unificadas_usd_sorted.csv'
    engine = VCPEngine(csv_path)
    df_vcp = engine.build_daily_vcp()
    
    out_path = r'c:\Users\tomas\white_finance\data\analytics\portfolio_vcp_history.csv'
    df_vcp.to_csv(out_path, index=False)

if __name__ == "__main__":
    main()
