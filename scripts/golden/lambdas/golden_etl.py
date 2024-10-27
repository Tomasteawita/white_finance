import json


class GoldenETL():

    def __init__(self, event):
        self.event = event


def lambda_handler(event, context):
    # TODO implement
    return {
        'statusCode': 200,
        'body': json.dumps('Hello from Lambda!')
    }
