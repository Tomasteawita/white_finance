"""
This module contains the template function to structure the raw portfolio data
"""
from importlib import import_module
from pandas import read_html
import boto3
from io import StringIO
import json

def structure_bronze_data_to_silver_template(transform):
    """
    Structure the raw portfolio data and save it in a csv file
    on silver layer.

    Args:
    - transform: Function that structures the raw portfolio data

    Side effects:
    - Save the structured portfolio data in a csv file
    """
    def execute(event, broker):
        s3 = boto3.client('s3')
        source_bucket = event['Records'][0]['s3']['bucket']['name']
        source_key = event['Records'][0]['s3']['object']['key']

        html_obj = s3.get_object(Bucket=source_bucket, Key=source_key)
        html_content = html_obj['Body'].read().decode('utf-8')

        html_buffer = StringIO(html_content)
        df_raw_portfolio = read_html(html_buffer, encoding='utf-8')[0]

        df_portfolio = transform(df_raw_portfolio)

        csv_buffer = StringIO()

        df_portfolio.to_csv(csv_buffer, index=False)

        destination_key = source_key.replace(
            f'bronze/portfolios/{broker}', f'silver/portfolios/{broker}'
        ).replace('.html', '.csv')
        destination_bucket = source_bucket

        s3.put_object(
            Bucket=destination_bucket,
            Key=destination_key,
            Body=csv_buffer.getvalue()
        )

    return execute

def set_transform(event):
    """Set the transform function based on the filename"""
    brokers = ['iol', 'bullma', 'balanz']

    for broker in brokers:
        if broker in event['Records'][0]['s3']['object']['key']:
            module = import_module(f'structure_portfolios.structure_{broker}_portfolio')
            function = getattr(module, 'structure_portfolio')
            return function, broker

    raise ValueError('No se pudo determinar la función de transformación')

def lambda_handler(event, context):
    """Main function that is executed when the lambda is triggered."""
    print(context)

    transform, broker = set_transform(event)
    silver_to_bronze = structure_bronze_data_to_silver_template(transform)
    silver_to_bronze(event, broker)

    return {
        'statusCode': 200,
        'body': json.dumps('Archivo cargado a silver')
    }
