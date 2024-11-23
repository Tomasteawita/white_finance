from jinja2 import Environment, FileSystemLoader
from reportlab.lib.pagesizes import letter
from reportlab.pdfgen import canvas
from bs4 import BeautifulSoup
import os
import sqlalchemy
import pandas as pd
import argparse
import warnings


warnings.filterwarnings("ignore")
print(os.getcwd())

def set_queries(workings_days_week, broker_name, partition_date_last, partition_date_first):

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

def gen_report(df_securities, broker_name):

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
    """Función que calcula el total de ganancias y pérdidas de todas las acciones de todos los brokers"""
    context['first_proffit'] = round(float(df_reports['ars_profit_first'].sum()),2)
    context['last_proffit'] = round(float(df_reports['ars_profit_last'].sum()),2)
    context['proffit_difference'] = round(context['last_proffit'],2) - round(context['first_proffit'],2)
    context['proffit_difference_percentage'] = round(((context['last_proffit'] - context['first_proffit']) / context['first_proffit']) * 100,2)

    return context

def html_to_pdf(html_content, output_path):
    # Crear un objeto canvas
    c = canvas.Canvas(output_path, pagesize=letter)
    width, height = letter

    # Parsear el contenido HTML
    soup = BeautifulSoup(html_content, 'html.parser')

    # Configurar la posición inicial
    y = height - 40
    for element in soup.find_all(['h1', 'h2', 'h3', 'p', 'li']):
        text = element.get_text()
        if element.name == 'h1':
            c.setFont("Helvetica-Bold", 16)
            c.drawString(40, y, text)
            y -= 20
        elif element.name == 'h2':
            c.setFont("Helvetica-Bold", 14)
            c.drawString(40, y, text)
            y -= 18
        elif element.name == 'h3':
            c.setFont("Helvetica-Bold", 12)
            c.drawString(40, y, text)
            y -= 16
        elif element.name == 'p':
            c.setFont("Helvetica", 10)
            c.drawString(40, y, text)
            y -= 14
        elif element.name == 'li':
            c.setFont("Helvetica", 10)
            c.drawString(60, y, f"- {text}")
            y -= 14

        if y < 40:
            c.showPage()
            y = height - 40

    c.save()


def gen_pdf_report(context, template_html_report_path, output_pdf_report_path):

    template_html_report_path = os.getcwd() + template_html_report_path.replace('./', '/')
    template_dir = os.path.dirname(template_html_report_path)
    template_file = os.path.basename(template_html_report_path)

    env = Environment(loader=FileSystemLoader(template_dir))


    template = env.get_template(template_file)

    for broker in context['brokers']:
        broker['df_report'] = broker['df_report'].to_html()

    rendered_html = template.render(context)
    html_to_pdf(rendered_html, output_pdf_report_path)

    print(f'Reporte generado en {output_pdf_report_path}')

def main(**kwargs):

    if kwargs.get('partition_date_report') is None:
        partition_date_report = pd.Timestamp.now().strftime('%Y-%m-%d')
    else:
        partition_date_report = kwargs.get('partition_date_report')

    print(f'Reporte de rentabilidad de la semana {partition_date_report}')

    context = {
        'partition_date_report': partition_date_report,
        'brokers' : [],
        'last_proffit' : None,
        'first_proffit' : None,
        'proffit_difference' : None,
        'proffit_difference_percentage' : None
    }

    user = os.environ['user']
    password = os.environ['password']
    host = os.environ['host']
    port = os.environ['port']
    database = os.environ['database']
    workings_days_week = kwargs.get('workings_days_week', 6)


    engine = sqlalchemy.create_engine(f'postgresql://{user}:{password}@{host}:{port}/{database}')

    bullma_query_profit_per_securitie = set_queries(
        workings_days_week, 'Bull Market',
        kwargs.get('partition_date_last'),
        kwargs.get('partition_date_first'))

    iol_query_profit_per_securitie = set_queries(
        workings_days_week, 'Invertir Online',
        kwargs.get('partition_date_last'), kwargs.get('partition_date_first')
    )

    with engine.connect() as conn:
        df_securities_bullma = pd.read_sql(bullma_query_profit_per_securitie, conn)
        df_securities_iol = pd.read_sql(iol_query_profit_per_securitie, conn)

    context['brokers'].append(gen_report(df_securities_bullma, 'Bull Market'))
    context['brokers'].append(gen_report(df_securities_iol, 'Invertir Online'))

    df_reports = pd.concat(
        [broker['df_report'] for broker in context['brokers']]
    )

    context = gen_total_profit(df_reports, context)

    gen_pdf_report(
        context,
        kwargs.get('template_html_report_path'),
        kwargs.get('output_pdf_report_path')
    )


if __name__ == "__main__":

    parser = argparse.ArgumentParser()
    parser.add_argument("--workings_days_week", type=int, help="6 default value")
    parser.add_argument("--partition_date_last", type=str, help="2024-11-11", default=None)
    parser.add_argument("--partition_date_first", type=str, help="2024-11-11", default=None)
    parser.add_argument("--partition_date_report", type=str, help="2024-11-11", default=None)
    parser.add_argument("--template_html_report_path", type=str, help="2024-11-11", default=None)
    parser.add_argument("--output_pdf_report_path", type=str, help="2024-11-11", default=None)

    args = parser.parse_args()

    main(**vars(args))

# python scripts/reports/profit_pdf_report.py --workings_days_week 5 --partition_date_first 2024-11-11 --partition_date_last 2024-11-15 --partition_date_report 2024-11-16 --template_html_report_path "./scripts/reports/template.html" --output_pdf_report_path "./scripts/reports/profit_report.pdf"