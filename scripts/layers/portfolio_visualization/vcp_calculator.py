import pandas as pd
import numpy as np
from sqlalchemy import create_engine
import os
from dotenv import load_dotenv

class VCPEngine:
    """
    Motor Mark-to-Market Vectorizado.
    Estructura el Valor Cuotaparte (VCP) aplicando operaciones matriciales sobre
    grillas temporales para aislar la gestión de riesgo Barbell. 
    """
    def __init__(self, csv_path: str):
        self.csv_path = csv_path
        
        # Diccionario de Ratios Oficiales
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
        
        # Clustering Estratégico
        self.fcis = ['ALGIIA', 'BMACTAA', 'BULL-IA', 'BULMAAA', 'RIGAHOR']
        self.safe_bonds = ['SNSBO', 'LK01Q'] 

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

    def _get_prices_from_db(self):
        """
        Extrae cotizaciones históricas y aplica conversión M2M y ajuste
        Full Share para CEDEARs excluyendo a YFinance.
        """
        engine = self._get_engine()
        query = """
        SELECT hp.date, ticker, 
            case when "source" <> 'YFinance_USD' then "close" / ccl else "close" end as close_usd,
            "source",
            ccl
        FROM earnings.historical_prices hp
        left join earnings.ccl_mep cm 
        on hp.date = cm."date";
        """
        df_px = pd.read_sql(query, engine)
        df_px['date'] = pd.to_datetime(df_px['date'])

        # Multiplicador Vectorizado para recuperar el valor Full Share
        def adjust_price(row):
            ticker = row['ticker']
            if ticker in self.ratios_cedear and row['source'] != 'YFinance_USD':
                return row['close_usd'] * self.ratios_cedear[ticker]
            return row['close_usd']

        df_px['precio_ajustado'] = df_px.apply(adjust_price, axis=1)
        return df_px

    def build_daily_vcp(self):
        # 1. Carga de Cuentas y Grilla Temporal
        df_cuentas = pd.read_csv(self.csv_path)
        df_cuentas['Operado'] = pd.to_datetime(df_cuentas['Operado'])
        
        min_date = df_cuentas['Operado'].min()
        max_date = pd.Timestamp.today().normalize()
        date_range = pd.date_range(start=min_date, end=max_date)

        # 2. Extracción de Precios y Curva de CCL
        df_px = self._get_prices_from_db()
        
        ccl_historico = df_px[['date', 'ccl']].drop_duplicates().set_index('date')
        ccl_historico = ccl_historico[~ccl_historico.index.duplicated()]
        ccl_grid = ccl_historico['ccl'].reindex(date_range).ffill().fillna(1000.0)

        df_prices_grid = df_px.pivot_table(index='date', columns='ticker', values='precio_ajustado')
        df_prices_grid = df_prices_grid.reindex(date_range).ffill().fillna(0)

        # 3. Filtrado y Signos Transaccionales
        valid_comprobantes = [
            'COMPRA EXTERIOR V', 'VENTA EXTERIOR V', 'COMPRA NORMAL', 'VENTA', 
            'COMPRA CAUCION CONTADO', 'VENTA CAUCION TERMINO', 'LICITACION PRIVADA',
            'COMPRA PARIDAD', 'VENTA PARIDAD', 'SUSCRIPCION FCI', 'RESCATE FCI'
        ]
        df_trades = df_cuentas[df_cuentas['Comprobante'].isin(valid_comprobantes)].copy()
        
        ventas_comprobantes = ['VENTA', 'VENTA EXTERIOR V', 'VENTA CAUCION TERMINO', 'VENTA PARIDAD', 'RESCATE FCI']
        df_trades['Signo_Cant'] = df_trades['Comprobante'].apply(lambda x: -1 if x in ventas_comprobantes else 1)
        df_trades['Flujo_Cant'] = df_trades['Cantidad'] * df_trades['Signo_Cant']

        # Enlazar CCL de la fecha operada para recuperar nominales en pesos originales
        df_trades = df_trades.merge(ccl_historico, left_on='Operado', right_index=True, how='left')
        df_trades['ccl'] = df_trades['ccl'].fillna(1000.0)

        # 4. Cash Dinámico (Liquidez)
        # Recuperamos el saldo ARS subyacente multiplicando por el CCL histórico
        # para luego dolarizarlo diariamente al CCL actual y evidenciar el FX Risk.
        df_saldos = df_cuentas.dropna(subset=['Saldo']).copy()
        df_saldos = df_saldos.merge(ccl_historico, left_on='Operado', right_index=True, how='left')
        df_saldos['ccl'] = df_saldos['ccl'].fillna(1000.0)
        
        df_saldos['Saldo_ARS'] = np.where(df_saldos['Origen'] == 'ARS', df_saldos['Saldo'] * df_saldos['ccl'], 0)
        
        cash_ars_grid = df_saldos.groupby('Operado')['Saldo_ARS'].last().reindex(date_range).ffill().fillna(0)
        cash_usd_pivot = df_saldos[df_saldos['Origen'] != 'ARS'].groupby(['Operado', 'Origen'])['Saldo'].last().unstack()
        cash_usd_grid = cash_usd_pivot.sum(axis=1).reindex(date_range).ffill().fillna(0)
        
        cash_total_usd = (cash_ars_grid / ccl_grid) + cash_usd_grid

        # 5. FCIs Conservadores (Safe Base Sintética)
        df_fci = df_trades[df_trades['Especie'].isin(self.fcis)].copy()
        df_fci['Flujo_ARS'] = np.where(df_fci['Origen'] == 'ARS', -df_fci['Importe'] * df_fci['ccl'], 0)
        df_fci['Flujo_USD'] = np.where(df_fci['Origen'] != 'ARS', -df_fci['Importe'], 0)

        fci_ars_grid = df_fci.groupby('Operado')['Flujo_ARS'].sum().reindex(date_range).fillna(0).cumsum()
        fci_usd_grid = df_fci.groupby('Operado')['Flujo_USD'].sum().reindex(date_range).fillna(0).cumsum()
        fci_m2m = (fci_ars_grid / ccl_grid) + fci_usd_grid

        # 6. Cauciones (Safe Base Sintética)
        df_cauc = df_trades[df_trades['Comprobante'].str.contains('CAUCION', na=False)].copy()
        df_cauc['Flujo_ARS'] = np.where(df_cauc['Origen'] == 'ARS', -df_cauc['Importe'] * df_cauc['ccl'], 0)
        df_cauc['Flujo_USD'] = np.where(df_cauc['Origen'] != 'ARS', -df_cauc['Importe'], 0)

        cauc_ars_grid = df_cauc.groupby('Operado')['Flujo_ARS'].sum().reindex(date_range).fillna(0).cumsum()
        cauc_usd_grid = df_cauc.groupby('Operado')['Flujo_USD'].sum().reindex(date_range).fillna(0).cumsum()
        cauc_m2m = (cauc_ars_grid / ccl_grid) + cauc_usd_grid

        # 7. Matrices de Mercado Libre (Renta Fija y Renta Variable)
        df_mercado = df_trades[~df_trades['Especie'].isin(self.fcis) & ~df_trades['Comprobante'].str.contains('CAUCION', na=False)]
        df_holdings = df_mercado.groupby(['Operado', 'Especie'])['Flujo_Cant'].sum().unstack(fill_value=0)
        df_holdings_grid = df_holdings.reindex(date_range).fillna(0).cumsum()

        # Operación Matricial M2M
        cols = df_holdings_grid.columns.intersection(df_prices_grid.columns)
        df_m2m = df_holdings_grid[cols].multiply(df_prices_grid[cols]).fillna(0)

        # 8. Segmentación y Agrupación de Riesgo Barbell
        safe_cols = [c for c in cols if any(s in c for s in self.safe_bonds)]
        growth_cols = [c for c in cols if c not in safe_cols]

        df_metrics = pd.DataFrame(index=date_range)
        df_metrics.index.name = 'Operado'
        
        df_metrics['Cash_Total_USD'] = cash_total_usd
        
        df_metrics['Total_Safe_Valuation'] = (
            df_m2m[safe_cols].sum(axis=1) + 
            fci_m2m + 
            cauc_m2m
        )
        
        df_metrics['Total_Growth_Valuation'] = df_m2m[growth_cols].sum(axis=1)
        
        df_metrics['Patrimonio_USD'] = (
            df_metrics['Cash_Total_USD'] + 
            df_metrics['Total_Safe_Valuation'] + 
            df_metrics['Total_Growth_Valuation']
        )

        print("\n--- Ejecución M2M Vectorizada ---")
        print(f"Patrimonio Inicial: US$ {df_metrics['Patrimonio_USD'].iloc[0]:,.2f}")
        print(f"Patrimonio Actual:  US$ {df_metrics['Patrimonio_USD'].iloc[-1]:,.2f}")

        return df_metrics.reset_index()

def main():
    csv_path = r'c:\Users\tomas\white_finance\data\analytics\cuentas_unificadas_usd_sorted.csv'
    engine = VCPEngine(csv_path)
    df_metrics = engine.build_daily_vcp()
    
    out_path = r'c:\Users\tomas\white_finance\data\analytics\portfolio_vcp_history.csv'
    df_metrics.to_csv(out_path, index=False)
    print(f"Dataset exportado con éxito a: {out_path}")

if __name__ == "__main__":
    main()