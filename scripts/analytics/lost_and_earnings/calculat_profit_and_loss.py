"""
Genera un dataframe con el restulado de ganancias/perdidas de operaciones de compra/venta de activos,
a partir de un archivo CSV, proveniente del excel que brinda BullMarket de nuestra cuenta corriente.
"""
import pandas as pd
import boto3
import io


def set_news_operations(df, max_date_yyyy_mm_dd):
    """
    Filtra las operaciones nuevas (novedades) que son posteriores a la fecha máxima dada.

    Args:
        df (pd.DataFrame): DataFrame con las operaciones.
        max_date_yyyy_mm_dd (str): Fecha máxima en formato 'YYYY-MM-DD'.

    Returns:
        pd.DataFrame: DataFrame filtrado con las novedades.
    """
    max_date = pd.to_datetime(max_date_yyyy_mm_dd)
    df['Operado'] = pd.to_datetime(df['Operado'], errors='coerce')
    return df[df['Operado'] > max_date]


def lambda_handler(event, context):
    """
    Calcula la ganancia o pérdida realizada para cada operación de venta
    de activos a partir de un archivo CSV de cuenta corriente.

    Args:
        ruta_archivo_csv (str): La ruta al archivo CSV exportado del broker.

    Returns:
        pandas.DataFrame: Un DataFrame con el resultado de cada venta.
    """
    bucket = event.get('bucket')
    key = event.get('key')

    s3 = boto3.client('s3')
    obj = s3.get_object(Bucket=bucket, Key=key)
    csv_data = obj['Body'].read()

    df_cuenta_corriente_historico = pd.read_csv(io.BytesIO(csv_data), 
                     delimiter=',',  # El delimitador parece ser coma
                     decimal='.')    # Usar punto como separador decimal
    obj = s3.get_object(Bucket= 'whitefinance-analytics', Key='profit.csv')
    csv_data = obj['Body'].read()
    df_profit = pd.read_csv(io.BytesIO(csv_data), 
                     delimiter=',',  # El delimitador parece ser coma
                     decimal='.')    # Usar punto como separador decimal
    # me quedo
    

    # Convertir columnas a los tipos de datos correctos
    df_cuenta_corriente_historico['Operado'] = pd.to_datetime(df_cuenta_corriente_historico['Operado'], format='mixed', dayfirst=True, errors='coerce')
    # Asegurarse que las columnas numéricas sean floats
    for col in ['Cantidad', 'Precio', 'Importe']:
        if df_cuenta_corriente_historico[col].dtype == 'object':
            df_cuenta_corriente_historico[col] = df_cuenta_corriente_historico[col].str.replace('.', '', regex=False).str.replace(',', '.', regex=False).astype(float)

    # 2. Filtrar solo operaciones de compra y venta
    operaciones = df_cuenta_corriente_historico[df_cuenta_corriente_historico['Comprobante'].isin(['COMPRA NORMAL', 'VENTA'])].copy()
    operaciones = operaciones.sort_values(by='Operado', ascending=True)

    # 3. Lógica principal: Iterar y calcular
    cartera = {}  # Diccionario para seguir el estado de cada activo
                    # Ejemplo: {'GGAL': {'cantidad': 100, 'costo_total': 45000}}
    resultados = [] # Lista para guardar los resultados de las ventas

    print("Procesando operaciones...")

    for index, op in operaciones.iterrows():
        especie = op['Especie']
        cantidad = op['Cantidad']
        importe = op['Importe']
        precio_op = op['Precio']

        # Inicializar el activo en la cartera si no existe
        if especie not in cartera and op['Comprobante'] == 'VENTA':
            print(f"ADVERTENCIA: Se intentó vender {cantidad} de {especie}, pero no hay registro de compra. Se omitirá.")
            continue

        elif especie not in cartera:
            cartera[especie] = {'cantidad_total': 0.0, 'costo_total': 0.0}

        # Si es una COMPRA
        if op['Comprobante'] == 'COMPRA NORMAL':
            cartera[especie]['cantidad_total'] += abs(cantidad)
            cartera[especie]['costo_total'] += abs(importe) # El importe de compra es negativo
            print(f"Compra: {cantidad:.2f} de {especie} a ${precio_op:.2f}")


        # Si es una VENTA
        elif op['Comprobante'] == 'VENTA':
            if abs(cartera[especie]['cantidad_total']) < abs(cantidad):
                print(f"ADVERTENCIA: Se intentó vender {cantidad} de {especie}, pero solo hay {cartera[especie]['cantidad_total']} en cartera. Se omitirá.")
                cartera.pop(especie, None)
                print(f"Se eliminó {especie} de la cartera por falta de cantidad suficiente.")
                continue

            # Calcular el Precio Promedio de Compra (PPC) al momento de la venta
            if cartera[especie]['cantidad_total'] > 0:
                ppc = cartera[especie]['costo_total'] / cartera[especie]['cantidad_total']
            else:
                ppc = 0 # Evitar división por cero

            # Calcular el costo de los activos vendidos
            costo_de_venta = ppc * cantidad
            # La ganancia es el importe de la venta (positivo) menos el costo
            ganancia_perdida = abs(importe) - abs(costo_de_venta)

            print(f"Venta: {cantidad:.2f} de {especie} a ${precio_op:.2f}. PPC: ${ppc:.2f}. Resultado: ${ganancia_perdida:.2f}")

            # Registrar el resultado de la operación
            resultados.append({
                'Fecha Venta': op['Operado'].date(),
                'Activo': especie,
                'Cantidad Vendida': cantidad,
                'Precio Venta': precio_op,
                'Precio Promedio Compra (PPC)': ppc,
                'Costo Total Venta': abs(costo_de_venta),
                'Ganancia/Perdida ($)': ganancia_perdida
            })

            # Actualizar la cartera después de la venta
            cartera[especie]['cantidad_total'] -= cantidad
            cartera[especie]['costo_total'] -= costo_de_venta

            # Si se vendió todo, se puede resetear el costo para evitar errores de flotantes
            if cartera[especie]['cantidad_total'] < 1e-9: # Un número muy pequeño
                cartera[especie]['cantidad_total'] = 0
                cartera[especie]['costo_total'] = 0


    print("\n¡Cálculo finalizado!")
    df_results =  pd.DataFrame(resultados)

    print(f"Se procesaron {len(resultados)} operaciones de venta.")
    return {
        'statusCode': 200,
        'body': json.dumps('Proceso de actualización de históricos completado exitosamente!'),
        # 'bucket': target_bucket,
        # 'key': target_key_historico
    }