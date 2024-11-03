from director.director import Director
from importlib import import_module
import json


def golden_etl(context, event):

    source_bucket = event['Records'][0]['s3']['bucket']['name']
    source_key = event['Records'][0]['s3']['object']['key']
    director = Director(source_bucket, source_key)

    if 'portfolios' in source_key:
        module = import_module('builders.stocks')
        stocks_class = getattr(module, 'Stocks')
        print("Entering stocks")
        builder_model = stocks_class(source_bucket, source_key)
    elif 'securities' in source_key:
        module = import_module('builders.aggregate_data')
        aggregate_data_class = getattr(module, 'AgreagateData')
        builder_model = aggregate_data_class(source_bucket, source_key)
    else:
        raise ValueError('No builder found for the given key')
    print("Setting builder")
    director.set_builder(builder_model)
    print("Constructing model")
    director.construct_model()

    return {
        'statusCode': 200,
        'body': json.dumps('Model constructed successfully')
    }





def lambda_handler(event, context):
    print(event)
    return golden_etl(context, event)
