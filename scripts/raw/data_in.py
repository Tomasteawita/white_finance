import json
import boto3

s3_client = boto3.client('s3')

def set_destination_key(source_key):
    """set the destination key based on the filename
    if the file has a broker, then it goes to raw/portfolios/broker_name
    if the file has interest_and_amortization, then it goes to raw/cashflows
    """
    # hago un ls a el source_bucket para traerme todos los paths

    source_key = source_key.replace('data/in/', '')
    table_path = source_key.split('-')[0]
    partition_data_path = source_key.split('-')[1]
    partition_data_path = f"{partition_data_path[:4]}-{partition_data_path[4:6]}-{partition_data_path[6:8]}"

    final_key = f'{table_path}/partition_date={partition_data_path}/{source_key}'

    return final_key

def data_in_to_raw(event):
    """Move the file from data/in to raw prefix based on the filename"""

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
        print(f'Error al copiar el archivo {source_key} a raw/{destination_key}')
        print(str(e))
        raise e

    s3_client.delete_object(Bucket=source_bucket, Key=source_key)
    print(f'Archivo {source_key} eliminado')
    return destination_key


def lambda_handler(event, context):
    """Main function that is executed when the lambda is triggered."""
    print("Contexto: " + str(context))
    destination_key = data_in_to_raw(event)

    return {
        'statusCode': 200,
        'body': json.dumps('Archivo copiado a raw'),
        'bucket-insertion': event['Records'][0]['s3']['bucket']['name'],
        'key-insertion': destination_key,
    }

# to test

# {
#   "Records": [
#     {
#       "eventVersion": "2.0",
#       "eventSource": "aws:s3",
#       "awsRegion": "us-east-1",
#       "eventTime": "1970-01-01T00:00:00.000Z",
#       "eventName": "ObjectCreated:Put",
#       "userIdentity": {
#         "principalId": "EXAMPLE"
#       },
#       "requestParameters": {
#         "sourceIPAddress": "127.0.0.1"
#       },
#       "responseElements": {
#         "x-amz-request-id": "EXAMPLE123456789",
#         "x-amz-id-2": "EXAMPLE123/5678abcdefghijklambdaisawesome/mnopqrstuvwxyzABCDEFGH"
#       },
#       "s3": {
#         "s3SchemaVersion": "1.0",
#         "configurationId": "testConfigRule",
#         "bucket": {
#           "name": "withefinance-raw",
#           "ownerIdentity": {
#             "principalId": "EXAMPLE"
#           },
#           "arn": "arn:aws:s3:::example-bucket"
#         },
#         "object": {
#           "key": "data/in/cuenta_corriente-20250718.csv",
#           "size": 1024,
#           "eTag": "0123456789abcdef0123456789abcdef",
#           "sequencer": "0A1B2C3D4E5F678901"
#         }
#       }
#     }
#   ]
# }