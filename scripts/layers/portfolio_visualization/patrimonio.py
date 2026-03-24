import pandas as pd
import numpy as np

def build_portfolio_metrics(df_trades, df_prices_sql, ratios_cedear):
    # 1. Definición del Universo Barbell (Clustering)
    # Renta Fija y Cauciones
    safe_assets = ['AL30', 'GD30', 'GD35', 'AE38', 'SNSBO', 'CAUCION'] 
    # El resto (Acciones, ETFs, CEDEARs, FCIs Abiertos) caerá en Growth
    
    # 2. Construir la Grilla Temporal (T)
    min_date = df_trades['Operado'].min()
    max_date = pd.Timestamp.today().normalize()
    date_range = pd.date_range(start=min_date, end=max_date)
    df_grid = pd.DataFrame(index=date_range)
    
    # ==========================================
    # COMPONENTE 1: MATRIZ DE TENENCIAS (T x N)
    # ==========================================
    # Asumiendo que df_trades tiene 'Operado', 'Especie', y 'Flujo_Cant' (Compras +, Ventas -)
    df_holdings = df_trades.groupby(['Operado', 'Especie'])['Flujo_Cant'].sum().unstack(fill_value=0)
    
    # Reindexar a la grilla completa y acumular
    df_holdings_grid = df_holdings.reindex(date_range).fillna(0).cumsum()
    
    # ==========================================
    # COMPONENTE 2: MATRIZ DE PRECIOS (T x N)
    # ==========================================
    df_px = df_prices_sql.copy()
    
    # Aplicar lógica de ajuste de precio para CEDEARs locales a Full Share USD
    def adjust_price(row):
        ticker = row['ticker']
        if ticker in ratios_cedear and row['source'] != 'YFinance_USD':
            return row['close_usd'] * ratios_cedear[ticker]
        return row['close_usd']
        
    df_px['precio_ajustado'] = df_px.apply(adjust_price, axis=1)
    
    # Pivotar para crear la matriz
    df_prices_grid = df_px.pivot_table(index='date', columns='ticker', values='precio_ajustado')
    df_prices_grid = df_prices_grid.reindex(date_range).ffill() # Arrastrar precios fines de semana
    
    # ==========================================
    # COMPONENTE 3: MARK-TO-MARKET (T x N)
    # ==========================================
    # Multiplicación elemento a elemento. Alinea automáticamente por fecha y ticker.
    df_m2m = df_holdings_grid.multiply(df_prices_grid).fillna(0)
    
    # ==========================================
    # COMPONENTE 4: CONSOLIDACIÓN DE MÉTRICAS
    # ==========================================
    df_metrics = pd.DataFrame(index=date_range)
    df_metrics.index.name = 'Operado'
    
    # 4.1 Liquidez (Cash_Total_USD)
    # Calculado a partir de saldos, transferencias y neteos diarios
    df_cash = df_trades.groupby('Operado')['Flujo_Importe'].sum().reindex(date_range).fillna(0).cumsum()
    df_metrics['Cash_Total_USD'] = df_cash
    
    # 4.2 Segmentación Safe vs Growth
    activos_presentes = df_m2m.columns
    safe_cols = [col for col in activos_presentes if col in safe_assets]
    growth_cols = [col for col in activos_presentes if col not in safe_assets]
    
    df_metrics['Total_Safe_Valuation'] = df_m2m[safe_cols].sum(axis=1)
    df_metrics['Total_Growth_Valuation'] = df_m2m[growth_cols].sum(axis=1)
    
    # 4.3 Patrimonio Final
    df_metrics['Patrimonio_USD'] = (
        df_metrics['Cash_Total_USD'] + 
        df_metrics['Total_Safe_Valuation'] + 
        df_metrics['Total_Growth_Valuation']
    )
    
    return df_metrics.reset_index()