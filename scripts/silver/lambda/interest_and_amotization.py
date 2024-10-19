import json
import pandas as pd
from io import BytesIO, StringIO
import boto3

def get_excel_information(excel_obj):
    """
    Get the excel file from the S3 bucket and return a object with 
    the sheets as keys and the dataframes as values
    """
    try:
        excel_content = excel_obj['Body'].read()
        excel_file = BytesIO(excel_content)
        return pd.read_excel(excel_file, sheet_name=None)
    except Exception as e:
        print(f"Error al leer el archivo Excel: {str(e)}")
        raise e

def generate_aggregated_data_json(df):
    """
    Generate the dictionary with par value data using pandas
    """
    try:
        par_value = df['Valor Par'].dropna().iloc[0]
        return {"par_value": par_value}
    except Exception as e:
        print(f"Error al generar datos agregados: {str(e)}")
        raise e

def save_file_s3(s3, file_buffer, destination_bucket, destination_key, sheet_name):
    """
    Save the file from a buffer
    """
    try:
        destination_key = destination_key.replace('interest_and_amortization', sheet_name)
        s3.put_object(
            Bucket=destination_bucket,
            Key=destination_key,
            Body=file_buffer.getvalue()
        )
    except Exception as e:
        print(f"Error al guardar el archivo en S3: {str(e)}")
        raise e

def put_interest_and_amortization_silver_layer(event):
    """
    Put the cashflows and aggregated data in the silver layer
    from interest and amortization bronze layer
    """
    s3 = boto3.client('s3')

    source_bucket = event['Records'][0]['s3']['bucket']['name']
    source_key = event['Records'][0]['s3']['object']['key']

    print(f"Bucket: {source_bucket}")
    print(f"Key: {source_key}")
    destination_bucket = source_bucket
    destination_fixed_income_cashflow_key = source_key.replace(
        'bronze/cashflows', 'silver/securities/fixed_income/cashflows'
    ).replace('.xlsx', '.csv')
    destination_fixed_income_aggregated_data_key = source_key.replace(
        'bronze/cashflows', 'silver/securities/fixed_income/aggregated_data'
    ).replace('.xlsx', '.json')

    try:
        sheets = get_excel_information(s3.get_object(Bucket=source_bucket, Key=source_key))
    except Exception as e:
        print(f"Error al obtener información del Excel: {str(e)}")
        raise e

    csv_buffer = StringIO()
    json_buffer = BytesIO()  # Cambiado a BytesIO

    for sheet_name, sheet_df in sheets.items():
        try:
            print(f"Hoja: {sheet_name}")
            dict_securitie = generate_aggregated_data_json(sheet_df)
            sheet_df.to_csv(csv_buffer, index=False, encoding='utf-8')
            save_file_s3(
                s3,
                csv_buffer,
                destination_bucket,
                destination_fixed_income_cashflow_key,
                sheet_name
            )
            print(f'Guardado {sheet_name}.csv')
            json_buffer.write(json.dumps(dict_securitie, ensure_ascii=False, indent=4).encode('utf-8'))
            json_buffer.seek(0)

            save_file_s3(
                s3,
                json_buffer,
                destination_bucket,
                destination_fixed_income_aggregated_data_key,
                sheet_name
            )
            print(f'Guardado {sheet_name}.json')
        except Exception as e:
            print(f"Error al procesar la hoja {sheet_name}: {str(e)}")
            raise e


def lambda_handler(event, context):
    try:
        put_interest_and_amortization_silver_layer(event)
    except Exception as e:
        print(f"Error en lambda_handler: {str(e)}")
        raise e