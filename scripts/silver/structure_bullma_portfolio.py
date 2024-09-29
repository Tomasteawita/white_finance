"""_summary_"""
from argparse import ArgumentParser
from dotenv import load_dotenv
from pandas import DataFrame
from raw_to_structure_template import structure_html_data_to_csv_template
load_dotenv()

def structure_portfolio(html_data):
    """
    Structure the raw portfolio data and save it in a csv file
    on silver layer.

    Side effects:
    - Save the structured portfolio data in a csv file
    """
    df_portfolio = DataFrame(html_data)

    df_portfolio = df_portfolio.drop(
        columns=[
            'Unnamed: 0', 'Unnamed: 12', 'Costo', 
            'Total Costo','Gan-Per %', 'Gan.-Per $',
            'Var % Diaria','Var $ Diaria'
            ]
        )

    df_portfolio['Cantidad'] = df_portfolio['Cantidad'].str.replace(',', '')
    df_portfolio = df_portfolio.loc[df_portfolio['Cantidad'].str.isnumeric()]
    df_portfolio['Cantidad'] = df_portfolio['Cantidad'].astype(int)
    df_portfolio['Cantidad'] = df_portfolio['Cantidad'] / 100

    columns_ars = ['Ultimo Precio', 'PPC', 'Total']

    for column in columns_ars:
        df_portfolio[column] = df_portfolio[column].str.replace(
                                    'ARS', ''
                                    ).str.replace(
                                    '.', ''
                                    ).str.replace(
                                        '.', ''
                                    ).str.replace(
                                        ',', '.'
                                    ).astype(float)

    df_portfolio['Producto'] = df_portfolio['Producto'].str.split('*')
    df_portfolio['Ticket'] = df_portfolio['Producto'].apply(lambda x: x[0]).str.strip()
    df_portfolio['Nombre'] = df_portfolio['Producto'].apply(lambda x: x[1]).str.strip()

    df_portfolio = df_portfolio.drop(columns=['Producto'])

    df_portfolio = df_portfolio[['Ticket', 'Nombre', 'Cantidad', 'Ultimo Precio', 'PPC', 'Total']]

    return df_portfolio


if __name__ == '__main__':

    parser = ArgumentParser()
    parser.add_argument(
        "--date_yyyymmdd", type=str, help="Raw portfolio date in YYYY-MM-DD format."
        )
    parser.add_argument(
        "--broker", type=str, help="Broker name; bullma or iol."
        )

    args = parser.parse_args()
    silver_layer_algorithm = structure_html_data_to_csv_template(structure_portfolio)
    silver_layer_algorithm(**vars(args))
