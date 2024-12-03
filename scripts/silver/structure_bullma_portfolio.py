"""Structure portfolio html file from Bull Market broker"""
from pandas import DataFrame


def set_currency(row):
    columns_ars = ['Ultimo Precio', 'PPC', 'Total']

    for column in columns_ars:
        if 'USD' in row[column].lower():
            return 'USD'
    
    return 'ARS'

def clean_currency(row):
    columns_ars = ['Ultimo Precio', 'PPC', 'Total']

    for column in columns_ars:
        row[column] = row[column].replace('ARS', '')
        row[column] = row[column].replace('USD', '')
        row[column] = row[column].replace('.', '')
        row[column] = row[column].replace(',', '.')
        row[column] = float(row[column])

    return row

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

    df_portfolio['Currency'] = df_portfolio.apply(set_currency, axis=1)
    df_portfolio = df_portfolio.apply(clean_currency, axis=1)

    df_portfolio['Producto'] = df_portfolio['Producto'].str.split('*')
    df_portfolio['Ticket'] = df_portfolio['Producto'].apply(lambda x: x[0]).str.strip()
    df_portfolio['Nombre'] = df_portfolio['Producto'].apply(lambda x: x[1]).str.strip()

    df_portfolio = df_portfolio.drop(columns=['Producto'])

    df_portfolio = df_portfolio[['Ticket', 'Nombre', 'Cantidad', 'Ultimo Precio', 'PPC', 'Total']]

    return df_portfolio
