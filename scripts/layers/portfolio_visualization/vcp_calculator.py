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
        
        # Opcionalidad de CEDEARs vs Undelying (Ratios Oficiales de Comafi)
        self.ratios_cedear = {
            'KO': 5.0, 'SPY': 20.0, 'QQQ': 20.0, 'AAPL': 10.0,
            'GOOGL': 58.0, 'MSFT': 30.0, 'TSLA': 15.0, 'MELI': 60.0,
            'LLY': 60.0, 'META': 24.0, 'VIST': 3.0, 'AMZN': 144.0,
            'NVDA': 24.0, 'NFLX': 60.0, 'TLT': 1.0, 'SH': 1.0,
            'ARGT': 1.0, 'XLP': 1.0, 'SHY': 1.0
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
        # Algunos bonos en dolares terminan en D ej AL30D, si removemos D cuidado con los cedears. Mejor dejamos limpiar .US y .BA
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
        df_trades['Flujo_Importe_USD'] = df_trades['Importe'].fillna(0.0) # Importe neteado

        # 2. Reestructuración de Matriz de Tiempos
        min_date = df_trades['Operado'].min()
        max_date = datetime.today().date()
        date_range = pd.date_range(start=min_date, end=max_date)
        
        df_grid = pd.DataFrame({'Operado': date_range})
        especies = df_trades['Especie_Base'].unique()
        
        # Traer saldos de liquidez (Cash en Dólares) unificados
        # Evaluamos el último saldo del día para las cuentas consolidadas.
        df_saldos = self.df_cuentas[['Operado', 'Origen', 'Saldo']].copy()
        df_saldos['Operado'] = pd.to_datetime(df_saldos['Operado'])
        df_saldos = df_saldos.dropna(subset=['Saldo'])
        df_cash_pivot = df_saldos.groupby(['Operado', 'Origen'])['Saldo'].last().unstack().reset_index()
        # Suma lineal del Cash USD elástico en todas las locaciones (Pesos, MEP, Cable)
        df_cash_pivot['Cash_Total_USD'] = df_cash_pivot.drop(columns=['Operado']).sum(axis=1)
        
        # CAUCIONES: las cauciones activas son Cash bloqueado temporalmente en plazo fijo.
        # Si no se suman al patrimonio mientras están activas, al vencimiento generan un
        # salto súbito de VCP (dinero que aparece de la nada). Se deben contabilizar
        # como un flujo neto: compra de caución suma +importe, venta de caución resta -importe.
        df_caucion = self.df_cuentas[
            self.df_cuentas['Comprobante'].isin(['COMPRA CAUCION CONTADO', 'VENTA CAUCION TERMINO'])
        ].copy()
        df_caucion['Operado'] = pd.to_datetime(df_caucion['Operado'])
        df_caucion = df_caucion.dropna(subset=['Importe'])
        # Importes: COMPRA CAUCION tiene Importe negativo (salida de cash hacia caución)
        # VENTA CAUCION tiene Importe positivo (vuelta de cash desde caución)
        # El neto acumulado representa el monto "en caución" en cada momento.
        df_caucion['Importe_Caucion'] = df_caucion['Importe']
        df_caucion_daily = df_caucion.groupby('Operado')['Importe_Caucion'].sum().reset_index()
        df_vcp_caucion = pd.merge(df_grid, df_caucion_daily, on='Operado', how='left')
        df_vcp_caucion['Importe_Caucion'] = df_vcp_caucion['Importe_Caucion'].fillna(0)
        # Posición neta de cauciones: negativa = está en caución (cash bloqueado)
        # La convertimos en positivo para que sea parte del patrimonio Safe Base
        df_vcp_caucion['Caucion_Activa_USD'] = (-df_vcp_caucion['Importe_Caucion'].cumsum()).clip(lower=0)
        
        # Merge de Cash con la Grilla (ffill para arrastrar flujo hasta el dia actual)
        df_vcp = pd.merge(df_grid, df_cash_pivot[['Operado', 'Cash_Total_USD']], on='Operado', how='left')
        df_vcp['Cash_Total_USD'] = df_vcp['Cash_Total_USD'].ffill()

        # 3. Traer Base de Datos Histórica (Precios PostgreSQL)
        engine = self._get_engine()
        query = "SELECT date, ticker, close FROM earnings.historical_prices"
        df_db = pd.read_sql(query, engine)
        df_db['date'] = pd.to_datetime(df_db['date'])
        
        df_prices = df_db.pivot_table(index='date', columns='ticker', values='close').reset_index()
        df_prices = pd.merge(df_grid, df_prices, left_on='Operado', right_on='date', how='left').drop(columns=['date'])
        
        # Traer Tipo de Cambio MEP
        query_fx = "SELECT date, mep FROM earnings.ccl_mep"
        df_fx = pd.read_sql(query_fx, engine)
        df_fx['date'] = pd.to_datetime(df_fx['date'])
        df_prices = pd.merge(df_prices, df_fx, left_on='Operado', right_on='date', how='left').drop(columns=['date'])
        
        # Extrapolar valuación de fines de semana y feriados (solo hacia adelante)
        # IMPORTANTE: NO usamos bfill(). Si un activo no tiene precio histórico previo,
        # se valúa como 0. bfill() propagaba precios futuros hacia el pasado, generando
        # sobrevaloraciones masivas en la fecha de compra de activos nuevos.
        df_prices.sort_values('Operado', inplace=True)
        df_prices = df_prices.ffill()

        # 4. Cálculo Iterativo de Equities (Holdings) + Barbell Clusters
        val_safe = np.zeros(len(df_grid))
        val_growth = np.zeros(len(df_grid))
        
        # Mapeo manual de CEDEARs si no están explícitos en el dict
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
                    
                # ALGORITMO ROBUSTO DE DETECCION DE DIVISA (PREVIENE SPIKES DE 200K)
                is_global_asset = (ticker_db in self.ratios_cedear) or (ticker_db in known_cedears)
                is_usd_db = False
                
                if is_global_asset:
                    mean_px = serie_precios[serie_precios > 0].mean()
                    if mean_px < 2500 and not pd.isna(mean_px):
                        is_usd_db = True
                
                if is_usd_db:
                    # Precio DB en USD puro. Debemos llevarlo al valor de 1 CEDEAR local.
                    ratio = self.ratios_cedear.get(ticker_db, 1.0)
                    if ticker_db == 'TLT': ratio = 1.0   
                    elif ticker_db == 'SPY': ratio = 20.0
                    elif ticker_db == 'QQQ': ratio = 20.0
                    elif ticker_db == 'SH': ratio = 1.0   
                    elif ticker_db == 'ARKK': ratio = 1.0
                    
                    precio_usd = serie_precios / ratio
                else:
                    # Activo local (AL30, GGAL, etc) OR CEDEARs scrapeados en ARS (ej: SH.BA)
                    # El precio ya refleja 1 unidad de CEDEAR (convertimos a USD con MEP)
                    factor_bono = 100.0 if any(b in ticker_db for b in self.bonds) else 1.0
                    mep_diario = df_prices['mep'].fillna(1000.0) 
                    precio_usd = (serie_precios / factor_bono) / mep_diario
                    
                val_diaria = merged['Holdings'] * precio_usd
                
                # Barbell Classification
                if any(s in ticker_db for s in self.safe_bonds):
                    val_safe += val_diaria.values
                else:
                    val_growth += val_diaria.values
                    
        # 5. Integración del VCP Final
        # El Total del Patrimonio = Safe Base (BonosCortos+FCIs+CASH+Cauciones) + Growth
        df_vcp = pd.merge(df_vcp, df_vcp_caucion[['Operado', 'Caucion_Activa_USD']], on='Operado', how='left')
        df_vcp['Caucion_Activa_USD'] = df_vcp['Caucion_Activa_USD'].fillna(0)
        
        df_vcp['Total_Safe_Valuation'] = val_safe + df_vcp['Cash_Total_USD'].fillna(0) + df_vcp['Caucion_Activa_USD']
        df_vcp['Total_Growth_Valuation'] = val_growth
        
        df_vcp['Valor_Cuotaparte_USD'] = df_vcp['Total_Safe_Valuation'] + df_vcp['Total_Growth_Valuation']
        
        print("\n--- Análisis de Clustering Barbell Exitoso ---")
        print(f"Patrimonio Total Inicial: US$ {df_vcp['Valor_Cuotaparte_USD'].iloc[0]:,.2f}")
        print(f"Patrimonio Total Actual:  US$ {df_vcp['Valor_Cuotaparte_USD'].iloc[-1]:,.2f}")
        
        return df_vcp

def main():
    csv_path = r'c:\Users\tomas\white_finance\data\analytics\cuentas_unificadas_usd_sorted.csv'
    engine = VCPEngine(csv_path)
    df_vcp = engine.build_daily_vcp()
    
    # Save the output Mark to Market Data
    out_path = r'c:\Users\tomas\white_finance\data\analytics\portfolio_vcp_history.csv'
    df_vcp.to_csv(out_path, index=False)
    print(f"Serie de tiempo guardada en: {out_path}")

if __name__ == "__main__":
    main()
