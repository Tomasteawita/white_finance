import json
from polars import read_excel, col
from io import BytesIO, StringIO
import boto3

def get_excel_information(excel_obj):
    """
    Get the excel file from the S3 bucket and return a object with 
    the sheets as keys and the dataframes as values
    """
    excel_content = excel_obj['Body'].read()
    excel_file = BytesIO(excel_content)

    return read_excel(excel_file, sheet_id=0)

def generate_aggregated_data_json(df):
    """
    Generate the dictionary with par value data
    """
    par_value = df.filter(
            col('Valor Par').is_not_null()
        ).select('Valor Par').head(1).get_column('Valor Par')[0]

    return {"par_value" : par_value}

def save_file_s3(s3, file_buffer, destination_bucket, destination_key, sheet_name):
    """
    Save the file from a buffer
    """
    destination_key = destination_key.replace('interest_and_amortization', sheet_name)
    s3.put_object(
        Bucket=destination_bucket,
        Key=destination_key,
        Body=file_buffer.getvalue()
    )

def put_interest_and_amortization_silver_layer(event):
    """
    Put the cashflows and aggregated data in the silver layer
    from interest and amortization bronze layer
    """
    s3 = boto3.client('s3')

    source_bucket = event['Records'][0]['s3']['bucket']['name']
    source_key = event['Records'][0]['s3']['object']['key']
    destination_bucket = source_bucket
    destination_fixed_income_cashflow_key = source_key.replace(
        'bronze/cashflows', 'silver/securities/fixed_income/cashflows'
    ).replace('.xlsx', '.csv')
    destination_fixed_income_aggregated_data_key = source_key.replace(
        'bronze/cashflows', 'silver/securities/fixed_income/aggregated_data'
    ).replace('.xlsx', '.json')

    sheets = get_excel_information(s3.get_object(Bucket=source_bucket, Key=source_key))

    csv_buffer = StringIO()
    json_buffer = StringIO()

    for sheet in sheets:
        print(f"Hoja: {sheet}")
        dict_securitie = generate_aggregated_data_json(sheets[sheet])
        sheets[sheet].write_csv(csv_buffer, separator=';')
        save_file_s3(
            s3,
            csv_buffer,
            destination_bucket,
            destination_fixed_income_cashflow_key,
            sheet
        )
        print(f'Guardado {sheet}.csv')
        json_buffer.write(json.dumps(dict_securitie, ensure_ascii=False, indent=4).encode('utf-8'))
        json_buffer.seek(0)

        save_file_s3(
            s3,
            json_buffer,
            destination_bucket,
            destination_fixed_income_aggregated_data_key,
            sheet
        )
        print(f'Guardado {sheet}.json')

def lambda_handler(event, context):
    """
    main function
    """
    print(context)
    put_interest_and_amortization_silver_layer(event)
    return {
        'statusCode': 200,
        'body': json.dumps('Hello from Lambda!')
    }
