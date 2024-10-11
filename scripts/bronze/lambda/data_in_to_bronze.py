import json
import boto3

s3_client = boto3.client('s3')


def set_destination_key(source_key):
    """set the destination key based on the filename
    if the file has a broker, then it goes to bronze/portfolios/broker_name
    if the file has interest_and_amortization, then it goes to bronze/cashflows
    """
    brokers = ['iol', 'bullma', 'balanz']

    for broker in brokers:
        if broker in source_key:
            destination_key = source_key.replace('data/in', f'bronze/portfolios/{broker}')
            return destination_key

    if 'interest_and_amortization' in source_key:
        destination_key = source_key.replace('data/in', 'bronze/cashflows')
        return destination_key

    return False

def data_in_to_bronze(event):
    """Move the file from data/in to bronze prefix based on the filename"""

    source_bucket = event['Records'][0]['s3']['bucket']['name']
    source_key = event['Records'][0]['s3']['object']['key']

    if not source_key.startswith('data/in/'):
        print(f'El archivo {source_key} no proviene del directorio data/in')
        return

    destination_bucket = source_bucket
    destination_key = set_destination_key(source_key)

    if destination_key is False:
        print(f'No se pudo determinar la carpeta de destino para el archivo {source_key}')
        raise ValueError('No se pudo determinar la carpeta de destino')

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

    s3_client.delete_object(Bucket=source_bucket, Key=source_key)
    print(f'Archivo {source_key} eliminado')


def lambda_handler(event, context):
    """Main function that is executed when the lambda is triggered."""
    print("Contexto: " + str(context))
    data_in_to_bronze(event)

    return {
        'statusCode': 200,
        'body': json.dumps('Archivo copiado a bronze')
    }
