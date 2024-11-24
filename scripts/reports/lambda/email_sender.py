"""
Reference to test send email: 
https://github.com/RekhuGopal/PythonHacks/blob/main/AWSBoto3Hacks/AWSBoto3-SES-Lambda.py
"""
import boto3
from botocore.exceptions import ClientError
from os import environ
import json

def send_email(event):
    sender = environ.get('SENDER_EMAIL')
    recipient = environ.get('RECIPIENT_EMAIL')

    s3 = boto3.client('s3')
    bucket_name_message = event.get('bucket_name_message')
    message_html_key = event.get('message_html_key')
    template_object = s3.get_object(Bucket=bucket_name_message, Key=message_html_key)
    message_html_content = template_object['Body'].read().decode('utf-8')

    aws_region = "us-east-2"

    subject = event.get('subject')

    body_html = message_html_content

    client = boto3.client('ses',region_name=aws_region)

    try:
        response = client.send_email(
            Destination={
                'ToAddresses': [
                    recipient,
                ],
            },
            Message={
                'Body': {
                    'Html': {
                        'Data': body_html
                    }
                },
                'subject': {

                    'Data': subject
                },
            },
            Source=sender
        )

    except ClientError as e:
        print(e.response['Error']['Message'])
    else:
        print("Email sent! Message ID:")
        print(response['MessageId'])

def lambda_handler(event, context):
    print("Received event: " + json.dumps(event, indent=2))
    print("to context: " + str(context))

    send_email(event)
    return {
        'statusCode': 200,
        'body': json.dumps('Hello from Lambda!')
    }

# probar armar un HTML pedorro en el bucket y mandar una prueba manual con ese bucket y key