"""
Example to test:
{
    "workings_days_week": 6,
    "partition_date_first": "2024-11-15",
    "partition_date_last": "2024-11-22",
    "partition_date_report": "2024-11-23",
    "bucket_report": "bucket",
    "key_report_template": "lambdas_path/reports/template.html"
}
"""
from jinja2 import Template
from sqlalchemy import create_engine
import pandas as pd
import warnings
from os import environ
import boto3
import json
warnings.filterwarnings("ignore")


def set_queries(workings_days_week, broker_name, partition_date_last, partition_date_first):
    """
    Setea la query para extraer:
    - ticket
    - precio promedio de compra (average_purchase_price)
    - ultimo precio (last_price)
    - cantidad (quantity)
    - tipo de instrumento financiero (financial_instrument_type)
    - fecha de particion (partition_date)
    de una cartera de inversiones de un broker en particular.
    """

    return f"""
        select securities.ticket , stocks.average_purchase_price , stocks.last_price,
            stocks.quantity, securities.financial_instrument_type, stocks.partition_date
        from golden.stocks stocks
        left join golden.securities securities
        on stocks.id_securitie = securities.id_securitie 
        left join golden.portfolios p 
        on p.id_portfolio = stocks.id_portfolio 
        where stocks.partition_date in (
            select partition_date from golden.portfolios p 
            where broker_name = '{broker_name}'
            {f"and partition_date between '{partition_date_first}' and '{partition_date_last}'" if partition_date_last else ''}
            order by partition_date desc
            limit {workings_days_week}
        )
        and p.broker_name = '{broker_name}'
        order by stocks.partition_date;
        """

def calculate_profits(df_securities):
    """
    Calcula las ganancias y perdidas de cada activo
    de una cartera de inversiones.
    """
    df_securities['average_purchase_price'] = df_securities.apply(
        lambda row: row['average_purchase_price'] / 100 if row['financial_instrument_type'] == 'renta fija' else row['average_purchase_price'],
        axis=1
    )
    df_securities['last_price'] = df_securities.apply(
        lambda row: row['last_price'] / 100 if row['financial_instrument_type'] == 'renta fija' else row['last_price'],
        axis=1
    )
    df_securities['ars_total'] = df_securities['last_price'] * df_securities['quantity']
    df_securities['ars_profit'] = df_securities['ars_total'] - (df_securities['average_purchase_price'] * df_securities['quantity'])
    df_securities['percentage_profit'] = (df_securities['ars_profit'] / (df_securities['average_purchase_price'] * df_securities['quantity'])) * 100

    return df_securities

def gen_broker_report(df_securities, broker_name):
    """
    Calcula las ganancias y perdidas de una cartera de inversiones
    especifica de un broker en particular.
    """
    profit_dict = {
        'name': broker_name,
        'first_partition_date': df_securities['partition_date'].iloc[0],
        'last_partition_date': df_securities['partition_date'].iloc[-1]
    }

    df_securities_first_day = df_securities[
        df_securities['partition_date'] == profit_dict['first_partition_date']
    ]
    df_securities_last_day = df_securities[
        df_securities['partition_date'] == profit_dict['last_partition_date']
    ]

    df_securities_first_day = calculate_profits(df_securities_first_day)
    df_securities_last_day = calculate_profits(df_securities_last_day)

    df_securities_merged = pd.merge(df_securities_first_day, df_securities_last_day, on='ticket', suffixes=('_first', '_last'))
    df_securities_merged['ars_profit_diff'] = df_securities_merged['ars_profit_last'] - df_securities_merged['ars_profit_first']

    df_report = df_securities_merged[['ticket', 'ars_profit_first', 'ars_profit_last', 'ars_profit_diff']]

    profit_dict['df_report'] = df_report
    profit_dict['best_securitie'] = df_report["ticket"].iloc[df_report["ars_profit_diff"].idxmax()]
    profit_dict['best_securitie_profit'] = round(df_report["ars_profit_diff"].max(),2)
    profit_dict['worst_securitie'] = df_report["ticket"].iloc[df_report["ars_profit_diff"].idxmin()]
    profit_dict['worst_securitie_profit'] = round(df_report["ars_profit_diff"].min(),2)

    df_securities_merged['percent_profit'] = \
        round((df_securities_merged['last_price_last'] - df_securities_merged['last_price_first']) / df_securities_merged['last_price_first'] * 100,2)
    df_report_percents = df_securities_merged[['ticket', 'percent_profit']]
    profit_dict['best_securitie_percent_profit'] = df_report_percents["ticket"].iloc[df_report_percents["percent_profit"].idxmax()]
    profit_dict['best_securitie_percent_profit_value'] = df_report_percents["percent_profit"].max()
    profit_dict['worst_securitie_percent_profit'] = df_report_percents["ticket"].iloc[df_report_percents["percent_profit"].idxmin()]
    profit_dict['worst_securitie_percent_profit_value'] = df_report_percents["percent_profit"].min()

    profit_dict['ars_profit_first'] = round(df_report['ars_profit_first'].sum(),2)

    profit_dict['ars_profit_last'] = round(df_report['ars_profit_last'].sum(),2)

    profit_dict['ars_profit_diff'] = round(profit_dict['ars_profit_last'] - profit_dict['ars_profit_first'],2)

    profit_dict['percentage_profit_diff'] = round(((profit_dict['ars_profit_last'] - profit_dict['ars_profit_first']) / profit_dict['ars_profit_first']) * 100,2)

    return profit_dict

def gen_total_profit(df_reports, context):
    """
    Función que calcula el total de ganancias y perdidas de todas las carteras de
    inversiones. Involucra a todos los brokers
    """
    context['first_proffit'] = round(float(df_reports['ars_profit_first'].sum()),2)
    context['last_proffit'] = round(float(df_reports['ars_profit_last'].sum()),2)
    context['proffit_difference'] = round(context['last_proffit'],2) - round(context['first_proffit'],2)
    context['proffit_difference_percentage'] = round(((context['last_proffit'] - context['first_proffit']) / context['first_proffit']) * 100,2)

    return context

def gen_html_report(context, template_html_content):
    """
    Funcion que escribe los datos en el template.html de reporte en formato HTML
    antes de enviarlo por email.
    """
    template = Template(template_html_content)

    for broker in context['brokers']:
        broker['df_report'] = broker['df_report'].to_html()

    return template.render(context)

def weekly_report(event):
    """
    Funcion principal que orquesta la generación del reporte de rentabilidad semanal
    de las carteras de inversiones de los brokers Bull Market e Invertir Online.
    """

    workings_days_week = event.get('workings_days_week', 6)

    if event.get('partition_date_report') is None:
        partition_date_report = pd.Timestamp.now().strftime('%Y-%m-%d')
    else:
        partition_date_report = event.get('partition_date_report')

    context = {
        'partition_date_report': partition_date_report,
        'brokers' : [],
        'last_proffit' : None,
        'first_proffit' : None,
        'proffit_difference' : None,
        'proffit_difference_percentage' : None
    }


    engine = create_engine(
        f'postgresql://{environ['POSTGRES_USER']}:{environ['POSTGRES_PASSWORD']}@{environ['POSTGRES_HOST']}:{environ['POSTGRES_PORT']}/{environ['POSTGRES_DATABASE']}'
        )

    bullma_query_profit_per_securitie = set_queries(
        workings_days_week, 'Bull Market',
        event.get('partition_date_last'),
        event.get('partition_date_first'))

    iol_query_profit_per_securitie = set_queries(
        workings_days_week, 'Invertir Online',
        event.get('partition_date_last'), event.get('partition_date_first')
    )

    with engine.connect() as conn:
        df_securities_bullma = pd.read_sql(bullma_query_profit_per_securitie, conn)
        df_securities_iol = pd.read_sql(iol_query_profit_per_securitie, conn)

    context['brokers'] = [
        gen_broker_report(df, broker_name) for df, broker_name in zip(
            [df_securities_bullma, df_securities_iol],
            ['Bull Market', 'Invertir Online']
        )
    ]

    df_reports = pd.concat(
        [broker['df_report'] for broker in context['brokers']]
    )

    context = gen_total_profit(df_reports, context)
    # deberia estar en el key lambda_path/reports/template.html
    s3 = boto3.client('s3')
    bucket_name = event.get('bucket_report')
    template_key = event.get('key_report_template')
    template_object = s3.get_object(Bucket=bucket_name, Key=template_key)
    template_html_content = template_object['Body'].read().decode('utf-8')

    template_rendered = gen_html_report(
        context,
        template_html_content
    )

    output_html_s3_report_bucket = event.get('bucket_report')
    output_html_s3_report_key = f"lambdas_path/reports/earnings_report_{partition_date_report}.html"

    s3.put_object(
        Bucket=output_html_s3_report_bucket,
        Key=output_html_s3_report_key,
        Body=template_rendered
    )

    print(f"Reporte generado en {output_html_s3_report_bucket}/{output_html_s3_report_key}")

    return output_html_s3_report_key

def lambda_handler(event, context):
    print("Received event: " + json.dumps(event, indent=2))
    print("to context: " + str(context))

    output_html_s3_report_key = weekly_report(event)
    return {
        'statusCode': 200,
        'body': json.dumps('Hello from Lambda!'),
        "sender_email": "tcueva.cloud@gmail.com",
        "recipient_email": "tcueva.cloud@gmail.com",
        "subject": "Weekly Earnings Report",
        "bucket_name_message": "portfolioslake",
        "message_html_key": output_html_s3_report_key
    }
