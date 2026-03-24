"""
Reference to test send email: 
https://github.com/RekhuGopal/PythonHacks/blob/main/AWSBoto3Hacks/AWSBoto3-SES-Lambda.py

example of event:
{
  "sender_email": "your_email@gmail.com",
  "recipient_email": "your_email@gmail.com",
  "subject": "test_mails",
  "message": "Mensaje Basico"
}
"""
import boto3
from botocore.exceptions import ClientError
import json

def send_email(event):
    sender = event.get('sender_email')
    recipient = event.get('recipient_email')
    aws_region = "us-east-2"
    subject = event.get('subject')
    message = event.get('message')

    # Validate required parameters
    if not all([sender, recipient, subject, message]):
        raise ValueError("Missing required parameters: sender_email, recipient_email, subject, or message")

    client = boto3.client('ses', region_name=aws_region)
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
                        'Data': message,
                        'Charset': 'UTF-8'
                    },
                    'Text': {
                        'Data': message,
                        'Charset': 'UTF-8'
                    }
                },
                'Subject': {  # Changed from 'subject' to 'Subject'
                    'Data': subject,
                    'Charset': 'UTF-8'
                },
            },
            Source=sender
        )

    except ClientError as e:
        print(f"ClientError: {e.response['Error']['Message']}")
        raise
    except Exception as e:
        print(f"Unexpected error: {str(e)}")
        raise
    else:
        print("Email sent! Message ID:")
        print(response['MessageId'])
        return response['MessageId']

def lambda_handler(event, context):
    print("Received event: " + json.dumps(event, indent=2))
    print("Context: " + str(context))

    try:
        message_id = send_email(event)
        return {
            'statusCode': 200,
            'body': json.dumps({
                'message': 'Email sent successfully!',
                'messageId': message_id
            })
        }
    except Exception as e:
        print(f"Error sending email: {str(e)}")
        return {
            'statusCode': 500,
            'body': json.dumps({
                'error': 'Failed to send email',
                'details': str(e)
            })
        }