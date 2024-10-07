import json
import boto3
import os

s3_client = boto3.client('s3')

def lambda_handler(event, context):
    print("Contexto: " + str(context))
    # Informacion del evento de carga en S3

    source_bucket = event['Records'][0]['s3']['bucket']['name']
    source_key = event['Records'][0]['s3']['object']['key']

    # Verifico que el archivo provenga del directorio portfolios/data/in

    if not source_key.startswith('data/in/'):
        print(f'El archivo {source_key} no proviene del directorio data/in')
        return

    # Definir el destino en la capa bronze
    destination_bucket = source_bucket # mismo bucket
    destination_key = source_key.replace('data/in', 'bronze')

    # copiar archivo a la carpeta bronze

    try:
        copy_source = {
            'Bucket': source_bucket,
            'Key': source_key
        }
        s3_client.copy_object(
            CopySource=copy_source,
            Bucket=destination_bucket,
            Key=destination_key
        )

    except Exception as e:
        print(f'Error al copiar el archivo {source_key} a bronze/{destination_key}')
        print(str(e))
        raise e

    # Eliminar archivo original

    s3_client.delete_object(Bucket=source_bucket, Key=source_key)
    print(f'Archivo {source_key} eliminado')

    return {
        'statusCode': 200,
        'body': json.dumps('Archivo copiado a bronze')
    }
