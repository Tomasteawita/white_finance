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
    df_iol_portfolio = DataFrame(html_data)

    df_iol_portfolio = df_iol_portfolio[["Activo", "Cantidad", "Último precio", "Precio promedio  de compra"]]

    df_iol_portfolio.columns = df_iol_portfolio.columns.droplevel(0)

    # Me quedo con la primera palabra de la columna Activo
    df_iol_portfolio['Activo'] = df_iol_portfolio['Activo'].str.split()
    df_iol_portfolio['Ticket'] = df_iol_portfolio['Activo'].str[0]

    # Elimino el primer elemento de la columna Activo
    df_iol_portfolio['Activo'] = df_iol_portfolio['Activo'].str[1:].str.join(' ')

    columns_ars = ["Precio promedio  de compra", "Último precio"]

    for column in columns_ars:
        df_iol_portfolio[column] = df_iol_portfolio[column].str.replace(
                                    '$', ''
                                ).str.replace(
                                    '.', ''
                                ).str.replace(
                                    ',', '.'
                                ).astype(float)

    df_iol_portfolio = df_iol_portfolio.rename(columns={
        "Activo": "Nombre",
        "Precio promedio  de compra": "PPC",
        "Último precio": "Ultimo Precio"
        })

    df_iol_portfolio['Total'] = df_iol_portfolio['Cantidad'] \
                                         * (df_iol_portfolio['Ultimo Precio'] / 100)

    df_iol_portfolio = df_iol_portfolio[[
        'Ticket', 'Nombre', 'Cantidad', 
        'Ultimo Precio', 'PPC', 'Total'
        ]]

    return df_iol_portfolio

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
