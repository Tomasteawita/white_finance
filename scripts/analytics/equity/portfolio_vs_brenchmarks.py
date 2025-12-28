import pandas as pd
import numpy as np
from datetime import datetime
from .gen_cartera_from_date import gen_cartera
import yfinance as yf

def main(df, fecha_inicio_str, fecha_fin_str):

    df['Liquida'] = pd.to_datetime(df['Liquida'])

    # Configuración de Fechas
    fecha_inicio = pd.to_datetime(fecha_inicio_str)  # 'YYYY-MM-DD'
    fecha_fin = pd.to_datetime(fecha_fin_str) # O la fecha de último dato

    # ---------------------------------------------------------
    # PASO A: CALCULAR CANTIDADES (HOLDINGS)
    # ---------------------------------------------------------
    def obtener_tenencia(df, fecha_corte):
        print(f'Fecha de corte: {fecha_corte.date()}')
        df_temp = df[df['Liquida'] < fecha_corte].copy()
        if df_temp.empty:
            return pd.Series(), 0.0

        # Ordenar por Liquida y Numero
        cash = df_temp.sort_values(['Liquida', 'Numero'])

        # Hacer row_number agrupado por 'Origen' (orden descendente para tomar el último)
        cash['row_number'] = cash.groupby('Origen').cumcount(ascending=False) + 1

        cash = cash[cash['row_number'] == 1]

        print("--- CASH POR ORIGEN ---")
        print(cash[['Origen', 'Liquida', 'Numero', 'Saldo']])
        cash = cash['Saldo'].sum()
        tenencia = gen_cartera(df_temp, fecha_corte=None)

        return tenencia, cash

    tenencia_ini, cash_ini = obtener_tenencia(df, fecha_inicio)
    tenencia_fin, cash_fin = obtener_tenencia(df, fecha_fin + pd.Timedelta(days=1))

    print("--- TENENCIA INICIAL A VALORIZAR ---")
    print(tenencia_ini)
    print(f"Cash Inicial: {cash_ini}")
    print("\n--- TENENCIA FINAL A VALORIZAR ---")
    print(tenencia_fin)
    print(f"Cash Final: {cash_fin}")

    # ---------------------------------------------------------
    # PASO B: VALORIZAR CARTERA CON YFINANCE
    # ---------------------------------------------------------
    def valorizar_cartera(tenencia, cash, fecha_referencia):
        """
        Valoriza la cartera usando yfinance para obtener precios.
        
        Args:
            tenencia (dict): Diccionario con estructura {'ticker': {'cantidad_total': float}}
            cash (float): Efectivo disponible
            fecha_referencia (pd.Timestamp): Fecha para obtener la cotización
            
        Returns:
            float: Valor total de la cartera en USD
        """
        valor_activos = 0.0
        fecha_str = fecha_referencia.strftime('%Y-%m-%d')
        df_cedears = pd.read_excel('../data/cedears shares.xlsx')['Identificación Mercado']
        # convierto la unica columna en una lista
        cedears_list = df_cedears.tolist()
        
        for ticker, data in tenencia.items():
            cantidad = data['cantidad_total']
            
            # Formatear el ticker para yfinance
            if ticker.endswith('.US'):
                # Quitar .US para tickers internacionales
                ticker_yf = ticker[:-3]
            elif ticker not in cedears_list:
                # Agregar .BA para tickers argentinos
                ticker_yf = f"{ticker}.BA"
            else:
                ticker_yf = ticker
            
            try:
                # Descargar datos históricos hasta la fecha de referencia
                stock = yf.Ticker(ticker_yf)
                hist = stock.history(start=fecha_referencia - pd.Timedelta(days=7), 
                                     end=fecha_referencia + pd.Timedelta(days=1))
                
                if hist.empty:
                    raise ValueError(f"No se encontraron datos para {ticker_yf}")
                
                # Obtener el precio de cierre más cercano a la fecha de referencia
                precio = hist['Close'].iloc[-1]
                valor_activos += cantidad * precio
                
                print(f"{ticker} ({ticker_yf}): {cantidad:.2f} x ${precio:.2f} = ${cantidad * precio:,.2f}")
                
            except Exception as e:
                print(f"Error al obtener precio para {ticker} ({ticker_yf}): {str(e)}")
        
        return valor_activos + cash

    v0 = valorizar_cartera(tenencia_ini, cash_ini, fecha_inicio)
    v1 = valorizar_cartera(tenencia_fin, cash_fin, fecha_fin)

    # finalizo el script para depurar
    return 0

    # ---------------------------------------------------------
    # PASO C: IDENTIFICAR FLUJOS DE CAJA (APORTES/RETIROS)
    # ---------------------------------------------------------
    # Filtramos movimientos que son dinero entrando/saliendo, no compra/venta de activos
    tipos_flujo = [
        'RECIBO DE COBRO', 'ORDEN DE PAGO', 'ORD PAGO DOLARES', 
        'REC COBRO DOLARES', 'NOTA DE CREDITO U$S', 'NOTA DE DEBITOS U$S'
    ]

    mask_periodo = (df['Liquida'] >= fecha_inicio) & (df['Liquida'] <= fecha_fin)
    df_flujos = df[mask_periodo & df['Comprobante'].isin(tipos_flujo)].copy()

    # Calculamos el peso temporal (Wi) para Dietz Modificado
    dias_totales = (fecha_fin - fecha_inicio).days
    df_flujos['Dias_Restantes'] = (fecha_fin - df_flujos['Liquida']).dt.days
    df_flujos['Peso_W'] = df_flujos['Dias_Restantes'] / dias_totales
    df_flujos['Flujo_Ponderado'] = df_flujos['Importe'] * df_flujos['Peso_W']

    f_neto = df_flujos['Importe'].sum()
    f_ponderado = df_flujos['Flujo_Ponderado'].sum()

    # ---------------------------------------------------------
    # PASO D: CÁLCULO FINAL Y COMPARATIVA
    # ---------------------------------------------------------

    # Rendimiento Dietz Modificado
    if v0 + f_ponderado != 0:
        rendimiento_portfolio = (v1 - v0 - f_neto) / (v0 + f_ponderado)
    else:
        rendimiento_portfolio = 0.0

    print(f"\n--- RESULTADOS ---")
    print(f"Valor Inicial (V0): ${v0:,.2f}")
    print(f"Valor Final (V1):   ${v1:,.2f}")
    print(f"Flujo Neto (F):     ${f_neto:,.2f}")
    print(f"Rendimiento Portfolio (YTD): {rendimiento_portfolio:.2%}")

    # --- COMPARATIVA CON MERVAL Y SP500 ---
    # Aquí usas yfinance para bajar los datos reales
    try:
        
        # Definir Tickers (Merval en USD suele usarse el ETF 'ARGT' o ajustar el índice Merval por CCL)
        # Aquí usaré SPY (S&P500) y ARGT (Proxy Merval/Argentina USD) como ejemplo
        tickers = ['SPY', 'ARGT'] 
        datos = yf.download(tickers, start=fecha_inicio, end=fecha_fin)['Adj Close']
        
        print("\n--- BENCHMARKS (USD) ---")
        for ticker in tickers:
            precio_i = datos[ticker].iloc[0]
            precio_f = datos[ticker].iloc[-1]
            retorno = (precio_f / precio_i) - 1
            print(f"{ticker}: {retorno:.2%}")
            
    except ImportError:
        print("\nLibrería yfinance no instalada. Instala con 'pip install yfinance'")