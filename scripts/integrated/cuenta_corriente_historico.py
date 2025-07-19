import json
import boto3
import pandas as pd
import io
import re
from datetime import datetime

# Inicializar el cliente de S3 fuera del handler para reutilizar la conexión
s3_client = boto3.client('s3')

def lambda_handler(event, context):
    """
    Procesa un archivo CSV de cuenta corriente, lo compara con datos históricos
    y actualiza el histórico con las nuevas transacciones (novedades).

    Pasos del proceso:
    1. Lee el archivo CSV recién cargado (definido en el evento).
    2. Determina la fuente de datos históricos:
       - Prioriza el archivo 'cuenta_corriente_historico.csv' en 'withefinance-integrated'.
       - Si no existe, busca la partición anterior más reciente en 'withefinance-raw'.
    3. Identifica las transacciones nuevas (novedades) comparando las fechas de operación.
    4. Concatena los datos históricos con las novedades.
    5. Escribe el DataFrame actualizado y consolidado en 'withefinance-integrated'.
    """

    # --- 1. Extraer parámetros del evento ---
    source_bucket = event['bucket']
    source_key = event['key']
    print(f"Iniciando proceso para: s3://{source_bucket}/{source_key}")

    # --- 2. Leer el nuevo CSV y convertirlo a DataFrame ---
    try:
        response = s3_client.get_object(Bucket=source_bucket, Key=source_key)
        # Asegurarse de que la columna 'Operado' se interprete como fecha
        df_new = pd.read_csv(response['Body'], parse_dates=['Operado'])
        print(f"Leído el archivo nuevo. {len(df_new)} filas encontradas.")
    except Exception as e:
        print(f"Error al leer el archivo desde s3://{source_bucket}/{source_key}. Error: {e}")
        raise e

    # --- Definir detalles del bucket y archivo de destino (histórico) ---
    target_bucket = 'withefinance-integrated'
    target_key_historico = 'cuenta_corriente_historico/cuenta_corriente_historico.csv'
    
    df_historical = pd.DataFrame() # DataFrame para almacenar los datos históricos

    # --- 3. Buscar el DataFrame histórico ---
    try:
        # Prioridad 1: Intentar leer el histórico consolidado desde 'withefinance-integrated'
        response_hist = s3_client.get_object(Bucket=target_bucket, Key=target_key_historico)
        df_historical = pd.read_csv(response_hist['Body'], parse_dates=['Operado'])
        print(f"Encontrado archivo histórico en 'integrated': s3://{target_bucket}/{target_key_historico}")

    except s3_client.exceptions.NoSuchKey:
        # Prioridad 2: El histórico no existe, buscar la partición anterior en 'withefinance-raw'
        print(f"Archivo histórico no encontrado en 'integrated'. Buscando partición anterior en 'raw'.")
        
        # Extraer la fecha de la partición actual del 'key'
        current_date_match = re.search(r'partition_date=(\d{4}-\d{2}-\d{2})', source_key)
        if not current_date_match:
            raise ValueError("No se pudo extraer 'partition_date' del key del evento.")
        
        current_partition_date = datetime.strptime(current_date_match.group(1), '%Y-%m-%d').date()
        print(f"Fecha de partición actual: {current_partition_date}")

        # Listar objetos en el bucket 'raw' para encontrar particiones anteriores
        paginator = s3_client.get_paginator('list_objects_v2')
        pages = paginator.paginate(Bucket=source_bucket, Prefix='cuenta_corriente/')

        previous_partitions = []
        for page in pages:
            for obj in page.get('Contents', []):
                key = obj['Key']
                match = re.search(r'partition_date=(\d{4}-\d{2}-\d{2})', key)
                if match:
                    partition_date = datetime.strptime(match.group(1), '%Y-%m-%d').date()
                    # Guardar solo las particiones anteriores a la actual
                    if partition_date < current_partition_date:
                        previous_partitions.append((partition_date, key))

        if previous_partitions:
            # Encontrar la máxima de las particiones anteriores
            latest_previous_partition = max(previous_partitions, key=lambda item: item[0])
            prev_partition_key = latest_previous_partition[1]
            print(f"Partición anterior encontrada: s3://{source_bucket}/{prev_partition_key}")

            # Leer el archivo CSV de la partición anterior
            response_prev = s3_client.get_object(Bucket=source_bucket, Key=prev_partition_key)
            df_historical = pd.read_csv(response_prev['Body'], parse_dates=['Operado'])
        else:
            # Caso base: No hay histórico en 'integrated' ni particiones anteriores en 'raw'
            print("No se encontraron particiones anteriores. Este es el primer archivo a procesar.")
            # df_historical permanecerá vacío

    # --- 4. Filtrar para obtener solo las novedades ---
    if not df_historical.empty:
        # Obtener la fecha máxima de operación del DataFrame histórico
        max_historical_date = df_historical['Operado'].max()
        print(f"Fecha máxima en el histórico: {max_historical_date}")
        
        # Filtrar el DataFrame nuevo para quedarse con fechas posteriores
        novedades_df = df_new[df_new['Operado'] > max_historical_date].copy()
        print(f"Se encontraron {len(novedades_df)} novedades.")

        # Combinar el histórico con las novedades
        df_final = pd.concat([df_historical, novedades_df], ignore_index=True)
    else:
        # Si no hay histórico, el DataFrame final es simplemente el nuevo DataFrame
        df_final = df_new
        print("No hay datos históricos; el archivo nuevo se convierte en la base del histórico.")

    # --- 5. Escribir el DataFrame final en el bucket 'integrated' ---
    if not df_final.empty:
        # Convertir DataFrame a formato CSV en memoria
        csv_buffer = io.StringIO()
        df_final.to_csv(csv_buffer, index=False)
        
        # Subir el archivo CSV a S3
        s3_client.put_object(
            Bucket=target_bucket,
            Key=target_key_historico,
            Body=csv_buffer.getvalue()
        )
        print(f"DataFrame final escrito exitosamente en s3://{target_bucket}/{target_key_historico}")
    else:
        print("No hay datos para escribir en el bucket de destino.")

    return {
        'statusCode': 200,
        'body': json.dumps('Proceso de actualización de históricos completado exitosamente!'),
        'bucket': target_bucket,
        'key': target_key_historico
    }

# {
#     "statusCode": 200,
#     "body": "Archivo copiado a raw",
#     "bucket": "withefinance-raw",
#     "key": "cuenta_corriente/partition_date=2025-07-18/cuenta_corriente-20250718.csv"
# }