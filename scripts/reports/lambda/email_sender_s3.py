"""
Reference to test send email: 
https://github.com/RekhuGopal/PythonHacks/blob/main/AWSBoto3Hacks/AWSBoto3-SES-Lambda.py

example of event:
{
  "sender_email": "your_email@gmail.com",
  "recipient_email": "your_email@gmail.com",
  "subject": "test_mails",
  "bucket_name_message": "bucket",
  "message_html_key": "key/path/message_test.html"
}
"""
import boto3
from botocore.exceptions import ClientError
import json

def send_email(event):
    sender = event.get('sender_email')
    recipient = event.get('recipient_email')

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
        'body': json.dumps('email sent!')
    }

