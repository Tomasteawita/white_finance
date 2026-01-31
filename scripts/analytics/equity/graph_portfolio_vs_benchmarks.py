import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
from .portfolio_vs_brenchmarks import obtener_tenencia
import yfinance as yf
# ---------------------------------------------------------
# 1. PREPARACIÓN DE DATOS (Simulación de tu entorno)
# ---------------------------------------------------------
# Cargar movimientos
def get_holdings():
    df = pd.read_csv('../data/cuentas_unificadas_sorted.csv')
    df['Liquida'] = pd.to_datetime(df['Liquida'])

    # Definir rango de fechas para "La Película"
    fecha_inicio = pd.to_datetime('2025-01-01')
    fecha_fin = df['Liquida'].max()
    all_dates = pd.date_range(start=fecha_inicio, end=fecha_fin, freq='D')

    # Identificar Flujos Externos (Dinero que entra/sale del cliente)
    tipos_flujo = ['RECIBO DE COBRO', 'ORDEN DE PAGO', 'ORD PAGO DOLARES', 
                'REC COBRO DOLARES', 'NOTA DE CREDITO U$S', 'NOTA DE DEBITOS U$S']

    # ---------------------------------------------------------
    # 2. RECONSTRUCCIÓN DIARIA DE TENENCIAS (Stock)
    # ---------------------------------------------------------
    # Crear un DataFrame base diario
    daily_portfolio = pd.DataFrame(index=all_dates)
    daily_portfolio.index.name = 'Fecha'

    # Función para obtener tenencia acumulada por día
    def get_daily_holdings(df, dates): # esta función genera resultados erronos, será corregida
        holdings_list = []
        cash_list = []
        
        # Saldos iniciales antes de la fecha de inicio
        mask_pre = df['Liquida'] < dates[0]
        if mask_pre.any():
            tenencia, last_cash = obtener_tenencia(
                df[mask_pre],
                dates[0]
            )
            current_holdings = {k: v['cantidad_total'] for k, v in tenencia.items()}
            del tenencia

        else:
            current_holdings = {}
            last_cash = 0.0

        # Iterar día por día (ineficiente pero claro para entender la lógica)
        df = df.sort_values(by=['Operado', 'Comprobante'])
        ratios_cedear = {'KO': 5.0} # Agrega SPY: 20, etc. si es necesario


        for d in dates:
            # Movimientos del día
            mask_day = df['Liquida'] == d
            day_moves = df[mask_day]
            
            if not day_moves.empty:
                # Actualizar Cash (tomamos el último saldo del día)
                # print(day_moves)
                # last_cash = day_moves.sort_values('Numero').iloc[-1]['Saldo']
                
                # Actualizar Tenencias
                for _, row in day_moves.iterrows():
                    if pd.notna(row['Especie']):
                        if row['Especie'].endswith('.US'):
                            especie_base = row['Especie'].replace('.US', '')
                            if especie_base in ratios_cedear:
                                row['Especie'] = especie_base
                                row['Cantidad'] = row['Cantidad'] * ratios_cedear[especie_base]
                        es_venta = row['Comprobante'] in ['VENTA', 'VENTA EXTERIOR V', 'VENTA CAUCION TERMINO', 'VENTA PARIDAD', 'CAUCION COLOCADORA TERMINO']
                        es_compra = row['Comprobante'] in ['COMPRA NORMAL', 'COMPRA EXTERIOR V', 'LICITACION PRIVADA', 'COMPRA CAUCION CONTADO', 'COMPRA PARIDAD', 'CAUCION COLOCADORA CONTADO']
                        if row['Especie'] not in current_holdings and es_venta:
                            # print(f"Omitiendo venta histórica de {row['Especie']}")
                            continue
                        if row['Especie'] not in current_holdings:
                            current_holdings[row['Especie']] = 0
                        if es_compra and row['Especie'] == 'VARIAS':
                            current_holdings[row['Especie']] += row['Cantidad']
                            last_cash += row['Importe']
                        elif es_compra:
                            current_holdings[row['Especie']] += row['Cantidad']
                            last_cash += row['Importe']
                        elif es_venta:
                            if current_holdings[row['Especie']] < abs(row['Cantidad']):
                                current_holdings[row['Especie']] = 0.0
                            else:
                                current_holdings[row['Especie']] -= abs(row['Cantidad'])
                            last_cash += row['Importe']

            # Guardar estado del día
            daily_state = current_holdings.copy()
            daily_state['CASH'] = last_cash
            print(f'Last cash on {d.date()}: {last_cash}')
            
            # Calcular Flujo Neto del día (Depositos - Retiros)
            # OJO: Solo sumamos los importes de los comprobantes tipo "Flujo"
            daily_flow = day_moves[day_moves['Comprobante'].isin(tipos_flujo)]['Importe'].sum()
            print(f'Net flow on {d.date()}: {daily_flow}')
            daily_state['NET_FLOW'] = daily_flow
            if daily_state['CASH'] + daily_flow < 0:
                daily_state['CASH'] = 0.0
            else:
                daily_state['CASH'] += daily_flow
            
            holdings_list.append(daily_state)
        
        return pd.DataFrame(holdings_list, index=dates)

    df_holdings = get_daily_holdings(df, all_dates)
    df_holdings = df_holdings.fillna(0) # Si no tienes un activo ese día, es 0

    return df_holdings, all_dates

def get_usa_prices(df_holdings):

    fechas = df_holdings.index
    start_date = df_holdings.index.min().strftime('%Y-%m-%d')
    end_date = (df_holdings.index.max() + pd.Timedelta(days=5)).strftime('%Y-%m-%d') # Unos días extra porsiacaso

    print(f"Buscando precios desde {start_date} hasta {end_date}...")

    # ---------------------------------------------------------
    # 2. MAPEO DE ACTIVOS (Diccionarios de Configuración)
    # ---------------------------------------------------------

    # GRUPO A: Activos que cotizan en EE.UU. (Cedears o Stocks)
    # La clave es tu nombre en el archivo, el valor es el ticker de Yahoo Finance
    tickers_usa = {
        # .US Stocks
        'TLT.US': 'TLT', 'INTC.US': 'INTC', 'SPY.US': 'SPY', 'SH.US': 'SH',
        'TSLA.US': 'TSLA', 'BULL-IA.US': 'BULL', 'ARGT.US': 'ARGT', 
        'XLP.US': 'XLP', 'SHY.US': 'SHY', 'UBT.US': 'UBT',
        
        # Cedears / Acciones Globales (Usamos el ticker original en USD)
        'KO': 'KO', 'VALE': 'VALE', 'HMY': 'HMY', 'SAN': 'SAN', 
        'UNH': 'UNH', 'AAPL': 'AAPL', 'MSFT': 'MSFT', 'PSQ': 'PSQ',
        'TSLA': 'TSLA', 'LLY': 'LLY', 'ASML': 'ASML', 'COIN': 'COIN',
        'ADBE': 'ADBE', 'MELI': 'MELI', 'GOOGL': 'GOOGL', 'VIST': 'VIST',
        'SH': 'SH' # Repetido en tu archivo
    }

    # GRUPO B: Activos Argentinos con ADR (Para usarlos directo en USD)
    tickers_adr = {
        'GGAL': 'GGAL', 'YPFD': 'YPF', 'CRES': 'CRESY', 
        'PAMP': 'PAM', 'EDN': 'EDN', 'CEPU': 'CEPU', 'TECO2': 'TEO'
    }

    # ---------------------------------------------------------
    # 3. DESCARGA DE DATOS
    # ---------------------------------------------------------
    df_prices_usd = pd.DataFrame(index=pd.to_datetime(fechas).sort_values())

    # A. Descargar y procesar Activos USD Directos
    print("Descargando Activos USA y ADRs...")
    lista_usa = list(tickers_usa.values()) + list(tickers_adr.values())
    # Añadimos GGAL.BA para calcular el CCL
    lista_completa = lista_usa

    data = yf.download(lista_completa, start=start_date, end=end_date)['Close']
    data = data.fillna(method='ffill')

    return data

def get_ars_prices(df_holdings):
    
    # Ir a investing.com o a https://iol.invertironline.com/titulo/datoshistoricos?simbolo=gd30&mercado=bcba para conseguir la cotización historicas de activos argentinos
    # https://iol.invertironline.com/Mercado/Cotizaciones
    # Ir a https://www.cafci.org.ar/comparadorAdministradora.html para conseguir la cotización historicas de fondos comunes de inversión argentinos
    tickers_local = {
        'SAMI': 'SAMI.BA', 'ALUA': 'ALUA.BA', 'AUSO': 'AUSO.BA',
        'MIRG': 'MIRG.BA', 'GBAN': 'GBAN.BA', 'DGCU2': 'DGCU2.BA',
        'TRAN': 'TRAN.BA', 'METR': 'METR.BA', 'BIOX': 'BIOX.BA',
        'BYMA': 'BYMA.BA', 'ECOG': 'ECOG.BA', 'FIPL': 'FIPL.BA',
        'MOLA': 'MOLA.BA', 'LEDE': 'LEDE.BA', 'CVH': 'CVH.BA',
        'AL30': 'AL30.BA', 'GD30': 'GD30.BA', 'GD35': 'GD35.BA', 'AE38': 'AE38.BA'
    }

    # inicializo el inicio y final de fechas

    start_date = df_holdings.index.min().strftime('%Y-%m-%d')
    end_date = (df_holdings.index.max() + pd.Timedelta(days=5)).strftime('%Y-%m-%d') # Unos días extra porsiacaso

    # D. Descargar y Convertir Activos Locales
    print("Descargando y Convirtiendo Activos Locales...")
    lista_local = list(tickers_local.values())
    data_local = yf.download(lista_local, start=start_date, end=end_date)['Close']
    data_local = data_local.fillna(method='ffill')

    # ---------------------------------------------------------
    # 4. MANEJO DE ACTIVOS FALTANTES (Manuales/Bonos raros)
    # ---------------------------------------------------------
    # Hay activos que Yahoo no tiene (FCI, Letras, etc). 
    # Los rellenaremos con 0 o un valor manual para que no rompa la multiplicación.
    # missing_cols = [c for c in df_holdings.columns if c not in df_prices_usd.columns and c not in ['Fecha', 'CASH', 'NET_FLOW']]

    # print(f"\n⚠️ Activos no encontrados (requieren precio manual): {missing_cols}")
    # for col in missing_cols:
    #     df_prices_usd[col] = 1.0 # Poner 1.0 temporalmente o 0.0

    # # CASH siempre vale 1 USD
    # df_prices_usd['CASH'] = 1.0

    # # ---------------------------------------------------------
    # # 5. GUARDAR
    # # ---------------------------------------------------------
    # df_prices_usd.index.name = 'Fecha'
    # df_prices_usd.to_csv('portfolio_daily_prices_usd.csv')
    # print("\n¡Listo! Archivo 'portfolio_daily_prices_usd.csv' generado.")

    return data_local

    # hago un stop para explorar el script

def portfolio_valuation_and_graph(df_holdings, all_dates):
    # ---------------------------------------------------------
    # 3. VALORIZACIÓN (Aquí necesitas tus precios históricos)
    # ---------------------------------------------------------
    # IMPORTANTE: Aquí debes conectar tu fuente de precios. 
    # df_prices debe tener índice de fechas y columnas por ticker (AL30, GGAL, etc) en USD.
    # Por ahora simularemos precios constantes para que el código corra.

    # --- ZONA DE CARGA DE PRECIOS DEL USUARIO ---
    # Ejemplo: df_prices = pd.read_csv('mis_precios_historicos.csv', index_col='Fecha')
    # Asegurate que estén en la misma moneda (USD CCL)
    # ------------------------------------------------

    # Simulación (Borrar esto cuando tengas tus precios):
    tickers_cartera = [c for c in df_holdings.columns if c not in ['CASH', 'NET_FLOW']]
    df_prices = pd.DataFrame(100.0, index=all_dates, columns=tickers_cartera) 
    # ------------------------------------------------

    # Calcular Patrimonio Total Diario (AUM)
    df_result = pd.DataFrame(index=all_dates)
    df_result['AUM'] = df_holdings['CASH'] # Arrancamos con el cash

    for ticker in tickers_cartera:
        # Multiplicar Cantidad x Precio
        # Usamos .reindex para asegurar que coincidan las fechas
        qty = df_holdings[ticker]
        px = df_prices[ticker].reindex(df_result.index).fillna(method='ffill')
        df_result['AUM'] += qty * px

    df_result['Flujo'] = df_holdings['NET_FLOW']

    # ---------------------------------------------------------
    # 4. CÁLCULO DE VALOR CUOTA (UNIT VALUE)
    # ---------------------------------------------------------
    # Inicializamos
    df_result['Cuotas'] = 0.0
    df_result['Valor_Cuota'] = 100.0 # Base 100

    # Lógica Iterativa de Cuotapartes
    # Día 0
    val_inicial = df_result['AUM'].iloc[0]
    if val_inicial > 0:
        df_result.loc[df_result.index[0], 'Cuotas'] = val_inicial / 100.0
    else:
        df_result.loc[df_result.index[0], 'Cuotas'] = 0 # Caso borde cartera vacía

    for i in range(1, len(df_result)):
        fecha_hoy = df_result.index[i]
        fecha_ayer = df_result.index[i-1]
        
        # Datos de Hoy
        aum_hoy = df_result.loc[fecha_hoy, 'AUM']
        flujo_hoy = df_result.loc[fecha_hoy, 'Flujo']
        cuotas_ayer = df_result.loc[fecha_ayer, 'Cuotas']
        valor_cuota_ayer = df_result.loc[fecha_ayer, 'Valor_Cuota']
        
        # Calcular Valor Pre-Flujo
        # Asumimos que el Flujo ocurre al cierre, o usamos el precio de ayer para suscribir
        # Metodología estándar: Suscripción al precio de cierre del día anterior (o del día si es realtime)
        # Aquí usaremos: Valor Cuota se calcula sobre el patrimonio ANTES del flujo del día
        
        patrimonio_pre_flujo = aum_hoy - flujo_hoy
        
        if cuotas_ayer > 0:
            nuevo_valor_cuota = patrimonio_pre_flujo / cuotas_ayer
        else:
            nuevo_valor_cuota = 100.0 # Reinicio si estaba vacía
            
        # Calcular Nuevas Cuotas emitidas/rescatadas por el flujo
        nuevas_cuotas = 0
        if nuevo_valor_cuota > 0:
            nuevas_cuotas = flujo_hoy / nuevo_valor_cuota
            
        # Guardar
        df_result.loc[fecha_hoy, 'Valor_Cuota'] = nuevo_valor_cuota
        df_result.loc[fecha_hoy, 'Cuotas'] = cuotas_ayer + nuevas_cuotas

    # ---------------------------------------------------------
    # 5. GRAFICAR (La Película)
    # ---------------------------------------------------------
    # Normalizar Benchmarks a Base 100
    # benchmark_data['SPY_Rebased'] = (benchmark_data['SPY'] / benchmark_data['SPY'].iloc[0]) * 100

    plt.figure(figsize=(12, 6))
    plt.plot(df_result.index, df_result['Valor_Cuota'], label='Mi Cartera (Neto)', linewidth=2.5)

    # Aquí agregarías tus benchmarks
    # plt.plot(benchmark_data.index, benchmark_data['ARGT_Rebased'], label='ARGT (Merval USD)', alpha=0.7)
    # plt.plot(benchmark_data.index, benchmark_data['SPY_Rebased'], label='S&P 500', alpha=0.7)

    plt.title('Evolución de Valor de Cuotaparte (Base 100)')
    plt.ylabel('Valor Base 100')
    plt.grid(True, linestyle='--', alpha=0.5)
    plt.legend()
    plt.show()