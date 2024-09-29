"""
This module contains the template function to structure the raw portfolio data
"""
from os import getenv
from pandas import read_html
from dotenv import load_dotenv


def structure_html_data_to_csv_template(transform):
    """
    Structure the raw portfolio data and save it in a csv file
    on silver layer.

    Args:
    - transform: Function that structures the raw portfolio data

    Side effects:
    - Save the structured portfolio data in a csv file
    """
    def execute(**kwargs):

        load_dotenv()

        broker = kwargs.get('broker')
        date = kwargs.get('date_yyyymmdd')
        bronze_data_path = getenv('BRONZE_DATA_PATH')
        silver_data_path = getenv('SILVER_DATA_PATH')

        html_data = read_html(f'{bronze_data_path}/{broker}_portfolio_{date}.html', encoding='utf-8')[0]

        df_portfolio = transform(html_data)

        df_portfolio.to_csv(
            f'{silver_data_path}/{broker}_portfolio_{date}.csv', index=True
        )
    return execute
