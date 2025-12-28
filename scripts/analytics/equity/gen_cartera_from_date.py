import pandas as pd


def gen_cartera(df_cuenta_corriente, fecha_corte=None):
    """
    Genera la cartera de acciones a partir de un DataFrame de cuenta corriente:
    Parámetros:
    - df_cuenta_corriente (pd.DataFrame): DataFrame de Pandas con las operaciones de cuenta corriente unificada.
    - fecha_corte (str or pd.Timestamp, optional): Fecha límite para considerar las operaciones (opcional), formato 'YYYY-MM-DD'.
    """
    # 1. Cargar y Preprocesar
    # df_cuentas_unificadas = pd.read_csv(...)
    df_cuenta_corriente['Operado'] = pd.to_datetime(df_cuenta_corriente['Operado'])

    if fecha_corte:
        df_cuenta_corriente = df_cuenta_corriente[
            df_cuenta_corriente['Operado'] <= pd.to_datetime(fecha_corte)
        ].copy()

    # FILTRO
    df_cartera = df_cuenta_corriente[
        df_cuenta_corriente['Comprobante'].isin([
            'COMPRA EXTERIOR V', 'VENTA EXTERIOR V', 
            'COMPRA NORMAL', 'VENTA', 
            'COMPRA CAUCION CONTADO', 'VENTA CAUCION TERMINO',
            'LICITACION PRIVADA',
            'COMPRA PARIDAD', 'VENTA PARIDAD'
        ]) & ~df_cuenta_corriente['Comprobante'].str.contains('TRADING')
    ].copy()

    # 2. ORDENAMIENTO CRÍTICO (Soluciona KO y AL30)
    # Ordenamos por Fecha y luego por Comprobante. 
    # Al ser alfabético, 'COMPRA...' se procesa antes que 'VENTA...',
    # garantizando que el saldo exista antes de venderlo en operaciones intradía.
    df_cartera = df_cartera.sort_values(by=['Operado', 'Comprobante'])

    cartera = {} 
    ratios_cedear = {'KO': 5.0} # Agrega SPY: 20, etc. si es necesario

    print("Procesando operaciones...")

    for index, op in df_cartera.iterrows():
        especie = op['Especie']
        cantidad = abs(op['Cantidad'])
        comprobante = op['Comprobante']

        # --- A. NORMALIZACIÓN ---
        if especie.endswith('.US'):
            especie_base = especie.replace('.US', '')
            if especie_base in ratios_cedear:
                especie = especie_base
                cantidad = cantidad * ratios_cedear[especie_base]

        # --- B. DEFINICIÓN TIPO ---
        es_venta = comprobante in ['VENTA', 'VENTA EXTERIOR V', 'VENTA CAUCION TERMINO', 'VENTA PARIDAD']
        es_compra = comprobante in ['COMPRA NORMAL', 'COMPRA EXTERIOR V', 'LICITACION PRIVADA', 'COMPRA CAUCION CONTADO', 'COMPRA PARIDAD']

        # --- C. VALIDACIÓN INICIAL ---
        # Si intentamos vender algo que no existe en el diccionario, asumimos que es histórico y lo omitimos.
        if especie not in cartera and es_venta:
            # print(f"Omitiendo venta histórica de {especie}")
            continue

        # Inicializar si no existe
        if especie not in cartera:
            cartera[especie] = {'cantidad_total': 0.0}

        # --- D. EJECUCIÓN ---
        if es_compra:
            cartera[especie]['cantidad_total'] += cantidad
        
        elif es_venta:
            # CORRECCIÓN PAAS: Evitar saldos negativos
            if cartera[especie]['cantidad_total'] < cantidad:
                # Si vendemos más de lo que hay registrado, vaciamos la cuenta (asumimos el resto es histórico)
                # print(f"Venta de {especie} excede saldo registrado. Vaciando posición.")
                cartera[especie]['cantidad_total'] = 0.0
            else:
                cartera[especie]['cantidad_total'] -= cantidad

        # Limpieza de saldos residuales (cero técnico)
        if cartera[especie]['cantidad_total'] < 0.001:
            cartera.pop(especie, None)

    print("\n--- Cartera Final Corregida ---")
    for k, v in cartera.items():
        print(f"{k}: {v['cantidad_total']:.2f}")
    return cartera