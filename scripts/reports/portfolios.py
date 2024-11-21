import os
import sqlalchemy
import pandas as pd
import argparse
import warnings

warnings.filterwarnings("ignore")

def set_queries(workings_days_week, broker_name, partition_date_last):

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
            {f"and partition_date <= '{partition_date_last}'" if partition_date_last else ''}
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

def gen_report(df_securities):
    first_partition_date = df_securities['partition_date'].iloc[0]
    last_partition_date = df_securities['partition_date'].iloc[-1]

    print(f'First partition date: {first_partition_date}')
    print(f'Last partition date: {last_partition_date}')

    df_securities_first_day = df_securities[df_securities['partition_date'] == first_partition_date]
    df_securities_last_day = df_securities[df_securities['partition_date'] == last_partition_date]

    df_securities_first_day = calculate_profits(df_securities_first_day)
    df_securities_last_day = calculate_profits(df_securities_last_day)
    df_securities_merged = pd.merge(df_securities_first_day, df_securities_last_day, on='ticket', suffixes=('_first', '_last'))
    df_securities_merged['ars_profit_diff'] = df_securities_merged['ars_profit_last'] - df_securities_merged['ars_profit_first']
    df_report = df_securities_merged[['ticket', 'ars_profit_first', 'ars_profit_last', 'ars_profit_diff']]
    print(f"Tabla de rentabilidad por activo y comparativa {first_partition_date} vs {last_partition_date}")
    print(df_report)
    print(f'En la semana gane {df_report["ars_profit_diff"].sum():.2f} ARS')
    print(f'El activo que mas plata me hizo ganar es {df_report["ticket"].iloc[df_report["ars_profit_diff"].idxmax()]} con {df_report["ars_profit_diff"].max():.2f} ARS')
    print(f'El activo que mas plata me hizo perder es {df_report["ticket"].iloc[df_report["ars_profit_diff"].idxmin()]} con {df_report["ars_profit_diff"].min():.2f} ARS')
    df_securities_merged['percent_profit'] = (df_securities_merged['last_price_last'] - df_securities_merged['last_price_first']) / df_securities_merged['last_price_first'] * 100
    df_report_percents = df_securities_merged[['ticket', 'percent_profit']]
    print(f'El activo que mas subio de precio es {df_report_percents["ticket"].iloc[df_report_percents["percent_profit"].idxmax()]} con {df_report_percents["percent_profit"].max():.2f}%')
    print(f'El activo que mas bajo de precio es {df_report_percents["ticket"].iloc[df_report_percents["percent_profit"].idxmin()]} con {df_report_percents["percent_profit"].min():.2f}%')

    ars_profit_first = df_report['ars_profit_first'].sum()
    ars_profit_last = df_report['ars_profit_last'].sum()

    print(f'La semana anterior gane {ars_profit_first:.2f} ARS')
    print(f'En esta semana gane {ars_profit_last:.2f} ARS')
    print(f'{ars_profit_last - ars_profit_first:.2f} ARS mas que la anterior')
    print(f'es decir, un {((ars_profit_last - ars_profit_first) / ars_profit_first) * 100:.2f}% mas')

    return df_report

def print_total_profit(df_reports):
    print('Total de la Bolsa')

    ars_profit_first = df_reports['ars_profit_first'].sum()
    ars_profit_last = df_reports['ars_profit_last'].sum()

    print(f'La semana anterior gane {ars_profit_first:.2f} ARS')
    print(f'En esta semana gane {ars_profit_last:.2f} ARS')
    print(f'{ars_profit_last - ars_profit_first:.2f} ARS mas que la anterior')
    print(f'es decir, un {((ars_profit_last - ars_profit_first) / ars_profit_first) * 100:.2f}% mas')

def main(**kwargs):
    # Set the environment variable

    user = os.environ['user']
    password = os.environ['password']
    host = os.environ['host']
    port = os.environ['port']
    database = os.environ['database']
    workings_days_week = kwargs.get('workings_days_week', 6)


    engine = sqlalchemy.create_engine(f'postgresql://{user}:{password}@{host}:{port}/{database}')

    bullma_query_profit_per_securitie = set_queries(workings_days_week, 'Bull Market', kwargs.get('partition_date_last')) 
    iol_query_profit_per_securitie = set_queries(workings_days_week, 'Invertir Online', kwargs.get('partition_date_last'))
    
    with engine.connect() as conn:
        df_securities_bullma = pd.read_sql(bullma_query_profit_per_securitie, conn)
        df_securities_iol = pd.read_sql(iol_query_profit_per_securitie, conn)

    print('Bull Market')
    df_report_bullma = gen_report(df_securities_bullma)
    print('Invertir Online')
    df_report_iol = gen_report(df_securities_iol)

    # union de ambos dataframes
    df_reports = pd.concat([df_report_bullma, df_report_iol])

    print_total_profit(df_reports)


if __name__ == "__main__":

    parser = argparse.ArgumentParser()
    parser.add_argument("--workings_days_week", type=int, help="6 default value")
    parser.add_argument("--partition_date_last", type=str, help="2024-11-11", default=None)

    args = parser.parse_args()

    main(**vars(args))
