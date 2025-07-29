"""
Genera un dataframe con el restulado de ganancias/perdidas de operaciones de compra/venta de activos,
a partir de un archivo CSV, proveniente del excel que brinda BullMarket de nuestra cuenta corriente.
"""
import pandas as pd
import boto3
import io
import json

def set_context_event(event):
    """Si el contexto es de un stepFunction, entonces event va a ser event['Payload']"""
    # si event tiene la keu Pyload, entonces es un stepFunction
    if 'Payload' in event:
        return event['Payload']
    return event

def lambda_handler(event, context):
    """
    Calcula la ganancia o perdida realizada para cada operacion de venta
    de activos a partir de un archivo CSV de cuenta corriente.

    Args:
        ruta_archivo_csv (str): La ruta al archivo CSV exportado del broker.

    Returns:
        pandas.DataFrame: Un DataFrame con el resultado de cada venta.
    """
    event = set_context_event(event)
    bucket = event.get('bucket')
    key = event.get('key')
    s3 = boto3.client('s3')

    obj = s3.get_object(Bucket=bucket, Key=key)
    csv_data = obj['Body'].read()
    df_cuenta_corriente_historico = pd.read_csv(
                        io.BytesIO(csv_data),
                        delimiter=',',  
                        decimal='.'
                    )
    # Asegurarse que las columnas numericas sean floats
    for col in ['Cantidad', 'Precio', 'Importe']:
        if df_cuenta_corriente_historico[col].dtype == 'object':
            df_cuenta_corriente_historico[col] = df_cuenta_corriente_historico[col].str.replace(',', '', regex=False).astype(float)

    # 2. Filtrar solo operaciones de compra y venta
    operaciones = df_cuenta_corriente_historico[df_cuenta_corriente_historico['Comprobante'].isin(['COMPRA NORMAL', 'VENTA'])].copy()
    # operaciones = operaciones.sort_values(by='Operado', ascending=True)

    # 3. Logica principal: Iterar y calcular
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
            print(f"ADVERTENCIA: Se intento vender {cantidad} de {especie}, pero no hay registro de compra. Se omitira.")
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
                print(f"ADVERTENCIA: Se intento vender {cantidad} de {especie}, pero solo hay {cartera[especie]['cantidad_total']} en cartera. Se omitira.")
                cartera.pop(especie, None)
                print(f"Se elimino {especie} de la cartera por falta de cantidad suficiente.")
                continue

            # Calcular el Precio Promedio de Compra (PPC) al momento de la venta
            if cartera[especie]['cantidad_total'] > 0:
                ppc = cartera[especie]['costo_total'] / cartera[especie]['cantidad_total']
            else:
                ppc = 0 # Evitar division por cero

            # Calcular el costo de los activos vendidos
            costo_de_venta = ppc * abs(cantidad)
            # La ganancia es el importe de la venta (positivo) menos el costo
            ganancia_perdida = abs(importe) - abs(costo_de_venta)

            print(f"Venta: {cantidad:.2f} de {especie} a ${precio_op:.2f}. PPC: ${ppc:.2f}. Resultado: ${ganancia_perdida:.2f}")

            # Registrar el resultado de la operacion
            resultados.append({
                'Fecha Venta': op['Operado'],
                'Activo': especie,
                'Cantidad Vendida': cantidad,
                'Precio Venta': precio_op,
                'Precio Promedio Compra (PPC)': ppc,
                'Costo Total Venta': abs(costo_de_venta),
                'Ganancia/Perdida ($)': ganancia_perdida
            })

            # Actualizar la cartera despues de la venta
            cartera[especie]['cantidad_total'] -= abs(cantidad)
            cartera[especie]['costo_total'] -= abs(costo_de_venta)
            print(f"Actualizada cartera: {cartera[especie]['cantidad_total']} de {especie} restantes.")

            # Si se vendio todo, se elimina la especie de la cartera
            if cartera[especie]['cantidad_total'] <= 0:
                cartera.pop(especie, None)
                print(f"Se elimino {especie} de la cartera por venta total.")


    print("\n¡Calculo finalizado!")
    df_results =  pd.DataFrame(resultados)

    print(f"Se procesaron {len(resultados)} operaciones de venta.")
    if df_results.empty:
        print("No se encontraron ventas para procesar.")
        return {
            'statusCode': 200,
            'body': json.dumps('No se encontraron ventas para procesar.')
        }
    
    print(df_results)

    # Escribo el df_final en el bucket whitefinance-analytics, en el archivo profit.csv
    target_bucket = 'whitefinance-analytics'
    target_key_historico = 'profit.csv'
    csv_buffer = io.StringIO()
    df_results.to_csv(csv_buffer, index=False, decimal='.', float_format='%.2f')
    s3.put_object(Bucket=target_bucket, Key=target_key_historico, Body=csv_buffer.getvalue())
    print(f"Archivo {target_key_historico} actualizado en el bucket {target_bucket}.")
    return {
        'statusCode': 200,
        'body': json.dumps('Proceso de actualizacion de historicos completado exitosamente!'),
        'bucket': 'whitefinance-analytics',
        'key': 'profit.csv'
    }