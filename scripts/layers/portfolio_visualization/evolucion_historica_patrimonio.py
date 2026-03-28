import numpy as np
import pandas as pd
from sqlalchemy import create_engine, Engine
from typing import Dict
import os
from dotenv import load_dotenv

class EvolucionHistoricaPatrimonio:

    def __init__(self):
        load_dotenv()
        self.ratios_cedear = self.fetch_cedear_ratios()
        self.especies_expresadas_en_100_nominales = ['SNSBO', 'GD35', 'GD30', 'AL30', 'AE38', 'LK01Q']
        self.fcis_abiertos = ['ALGIIIA', 'BMACTAA', 'BULL-IA', 'BULMAAA', 'RIGAHOR']
        self.path_cuentas_unificadas = '../data/analytics/cuentas_unificadas_sorted.csv'
        self.db_uri = "postgresql://postgres:postgres@localhost:5432/postgres"

    def fetch_cedear_ratios(self) -> Dict[str, float]:
        """
        Extrae ratios de CEDEARs desde PostgreSQL para cálculos de paridad y valoración.
        
        Aplica lógica de limpieza para asegurar que cada ticker tenga un ratio válido
        antes de la conversión a diccionario.
        """
        # Construcción de conexión (ajusta según tus variables en .env)
        user = 'postgres'
        pw = 'postgres'
        host = 'localhost'
        db = 'postgres'
        
        engine: Engine = create_engine(f"postgresql://{user}:{pw}@{host}:5432/{db}")
        
        query: str = "SELECT ticker, ratio FROM earnings.ratios_cedears"
        
        try:
            # Lectura directa a DataFrame
            df: pd.DataFrame = pd.read_sql(query, engine)
            
            # Validación: Eliminar filas con nulos en columnas críticas
            df = df.dropna(subset=['ticker', 'ratio'])
            
            # Generación del diccionario {ticker: ratio}
            ratios_cedear: Dict[str, float] = df.set_index('ticker')['ratio'].to_dict()
            
            return ratios_cedear
            
        except Exception as e:
            print(f"CRITICAL: Error en la extracción de ratios: {e}")
            return {}
        finally:
            engine.dispose()

    def process_transactions(self, transactions_df):
        # Inventario actual en nominales y billetes
        portfolio = {'Cash_ARS': 0.0, 'Cash_MEP': 0.0, 'Cash_CCL': 0.0} 
        
        # Diccionario auxiliar para trackear el saldo anterior de cada cuenta corriente
        last_saldos = {'ARS': 0.0, 'USD MEP': 0.0, 'USD CCL': 0.0}
        
        daily_snapshots = []
        
        for _, row in transactions_df.iterrows():
            fecha = row['Operado']
            comp = row['Comprobante']
            origen = row['Origen']
            especie = str(row['Especie']) if pd.notna(row['Especie']) else None
            # Manejo seguro de NaN en Importe y Saldo
            importe = float(row['Importe']) if pd.notna(row['Importe']) else 0.0
            saldo_actual = float(row['Saldo']) if pd.notna(row['Saldo']) else 0.0
            numero = row['Numero']

            # if especie == 'AL30':
            #     print(f'Procesando {row}')

            # A. Actualización general de Saldos Líquidos (Cash)
            # if especie == 'VARIAS':
            #     print(f'{comp}, fecha: {fecha}, monto: {importe}, saldo_actual: {saldo_actual}')
            if origen == 'ARS': 
                portfolio['Cash_ARS'] = round(importe + portfolio['Cash_ARS'], 2)
            elif origen == 'USD MEP': 
                if comp == 'VENTA PARIDAD' and (portfolio['Cash_MEP'] + importe) != saldo_actual:        
                    portfolio['Cash_MEP'] = round(saldo_actual, 2)
                    continue
                else:
                    portfolio['Cash_MEP'] = round(portfolio['Cash_MEP'] + importe, 2)
            elif origen == 'USD CCL':
                portfolio['Cash_CCL'] = round(importe + portfolio['Cash_CCL'], 2)

            # B. Actualización de Activos (Nominales)
            if especie:
                cantidad = row['Cantidad']
                if especie.endswith('.US'):
                    ticker_unificado = especie.replace('.US','')
                elif especie in ratios_cedear:
                    cantidad = round(cantidad / ratios_cedear[especie], 2)
                    ticker_unificado = especie
                else:
                    ticker_unificado = especie
                
                if ticker_unificado not in portfolio:
                    portfolio[ticker_unificado] = 0.0

                if ticker_unificado in fcis_abiertos:
                    portfolio[ticker_unificado] += row['Importe'] * -1
                    portfolio[ticker_unificado] = 0.0 if portfolio[ticker_unificado] < 0 else portfolio[ticker_unificado]
                elif comp == 'VENTA PARIDAD' and (portfolio[ticker_unificado] + cantidad) <= 0:
                    portfolio[ticker_unificado] = 0.0
                elif comp == 'COMPRA PARIDAD' and (portfolio[ticker_unificado] + cantidad) == (cantidad * 2):
                    pass
                # elif comp == 'VENTA' and origen == 'ARS' and (portfolio[ticker_unificado] + cantidad) < 0:
                #     portfolio[ticker_unificado] = 0.0
                else:
                    portfolio[ticker_unificado] += round(cantidad, 2)
            
            # C. Actualizar el trackeo del saldo para la próxima iteración
            last_saldos[origen] = saldo_actual
            
            # Guardar estado diario
            snapshot = portfolio.copy()
            snapshot['Operado'] = fecha
            daily_snapshots.append(snapshot)
            # print(daily_snapshots)
            
        # Convertir a DataFrame y consolidar por día
        snapshots_df = pd.DataFrame(daily_snapshots)
        daily_balances = snapshots_df.groupby('Operado').last().reset_index()
        
        return daily_balances
    
    def get_market_data(self, engine_uri):
        engine = create_engine(engine_uri)
        query = """
            SELECT hp.date, ticker,
            case when "source" <> 'YFinance_USD' then "close" / ccl else "close" end as close_usd,
            case when "source" = 'YFinance_USD' then "close" * ccl else "close" end as close_ars,
            "source",
            ccl
            FROM earnings.historical_prices hp
            left join earnings.ccl_mep cm
            on hp.date = cm."date";
        """
        df_prices = pd.read_sql(query, engine)

        mask = (df_prices['ticker'].isin(self.ratios_cedear.keys())) & (df_prices['source'] != 'YFinance_USD')
        df_prices.loc[mask, 'close_usd'] = df_prices.loc[mask, 'close_usd'] * df_prices.loc[mask, 'ticker'].map(self.ratios_cedear)
        # 2. Transformación de Bonos y Letras (de cada 100 nominales a valor unitario)
        mask_bonos = df_prices['ticker'].isin(self.especies_expresadas_en_100_nominales)
        
        # Dividimos el precio en USD por 100 para estandarizar la cotización
        df_prices.loc[mask_bonos, 'close_usd'] = df_prices.loc[mask_bonos, 'close_usd'] / 100.0

        return df_prices
    
    def run(self):
        df = pd.read_csv(
            self.path_cuentas_unificadas,
            parse_dates=['Operado', 'Liquida']
        )
        df = df.sort_values(by=['Operado', 'Numero']).reset_index(drop=True)
        holdings_diarios = self.process_transactions(df)
        holdings_diarios.set_index('Operado', inplace=True)

        # Completar días sin operaciones (Forward Fill)
        idx = pd.date_range(holdings_diarios.index.min(), pd.Timestamp.today())
        holdings = holdings_diarios.reindex(idx, method='ffill').fillna(0)
        df_prices  = get_market_data(self.db_uri)
        serie_ccl = df_prices.drop_duplicates(subset=['date']).set_index('date')['ccl']

        precios_pivot = df_prices.pivot(index='date', columns='ticker', values='close_usd')
        precios_pivot = precios_pivot.add_suffix('_price')
        precios_matriz = precios_pivot.reindex(holdings.index)
        serie_ccl = serie_ccl.reindex(holdings.index)
        precios_matriz = precios_matriz.ffill()
        serie_ccl = serie_ccl.ffill()
        precios_matriz['CCL'] = serie_ccl
        holdings_columns_set = set(holdings.columns)
        holdings_columns_to_calculate_total = holdings_columns_set.difference(set(fcis_abiertos))
        holdings_columns_to_calculate_total = holdings_columns_to_calculate_total.difference({'Cash_ARS','Cash_CCL','Cash_MEP', 'VARIAS', 'MEP'})

        df_consolidado = pd.concat([holdings, precios_matriz], axis=1)

        for column in holdings_columns_to_calculate_total:
            df_consolidado[column] = df_consolidado[column] * df_consolidado[f'{column}_price']

        df_consolidado['Cash_ARS'] = df_consolidado['Cash_ARS'] / df_consolidado['CCL']

        for fci in ['ALGIIIA', 'BMACTAA', 'BULMAAA', 'RIGAHOR']:
            df_consolidado[fci] = df_consolidado[fci] / df_consolidado['CCL']

        sufijo = '_price'
        columnas_a_borrar = [col for col in df_consolidado.columns if col.endswith(sufijo)] + ['MEP']
        df_consolidado = df_consolidado.drop(columns=columnas_a_borrar)
        total_assets = set(df_consolidado.columns)

        safe_assets = {'SNSBO', 'GD35', 'GD30', 'AL30', 'AE38', 'LK01Q','BMACTAA'}
        cash = {'Cash_ARS', 'Cash_MEP', 'Cash_CCL'}
        growth_assets = total_assets.difference(safe_assets)
        growth_assets = growth_assets.difference(cash)
        growth_assets.discard('CCL')
        safe_assets = list(safe_assets)
        cash = list(cash)
        growth_assets = list(growth_assets)

        df_consolidado['Cash_Total_USD'] = df_consolidado[cash].sum(axis=1)
        df_consolidado['Total_Safe_Valuation'] = df_consolidado[safe_assets].sum(axis=1)
        df_consolidado['Total_Growth_Valuation'] = df_consolidado[growth_assets].sum(axis=1)
        df_consolidado['Patrimonio_USD'] = df_consolidado[['Cash_Total_USD', 'Total_Safe_Valuation', 'Total_Growth_Valuation']].sum(axis=1)
        df_consolidado.index.name = 'Fecha'
        df_consolidado[['Cash_Total_USD', 'Total_Safe_Valuation', 'Total_Growth_Valuation', 'Patrimonio_USD']].to_csv(
            '../../../data/analytics/portfolio_visualization_data/evolucion_patrimonio.csv'
        )
