import pandas as pd
import boto3
import io

def lambda_handler(event, context):
    """
    Lambda function to read a CSV file from S3 and perform some operations.
    """
    bucket_name = 'whitefinance-integrated'
    key = 'cuenta_corriente_historico/cuenta_corriente_historico.csv'

    try:
        s3 = boto3.client('s3')
        obj = s3.get_object(Bucket=bucket_name, Key=key)
        csv_data = obj['Body'].read()

        df = pd.read_csv(io.BytesIO(csv_data))

        # Perform your data processing here.  For example, print the head of the DataFrame:
        print(df.head())

        # Return a success message
        return {
            'statusCode': 200,
            'body': 'Successfully processed CSV from S3!'
        }

    except Exception as e:
        print(f"Error processing CSV from S3: {e}")
        return {
            'statusCode': 500,
            'body': f"Error: {str(e)}"
        }
