"""transform html data to structured data"""
from pandas import DataFrame

def define_type_security(security_name):
    """
    Define the type of security based on the security name.

    Args:
    - security_name: str

    Returns:
    - str
    """
    bonos = ['Bono', 'Bopreal', 'Vto']

    on = ['On', 'Obligacion negociable','Vto.']

    letras = ['Letra', 'Lete', 'Lecap', 'Lelink']

    if any(word in security_name for word in bonos):
        return 'bono'
    if any(word in security_name for word in on):
        return 'obligacion_negociable'
    if any(word in security_name for word in letras):
        return 'letra'

    return 'Instrumento_no_identificado'

def calculate_total(row):
    """
    Calculate the total amount of the security.

    Args:
    - row: pd.Series

    Returns:
    - float
    """
    print(f"Nombre: {row['Nombre']}")
    print(f"Tipo: {row['Tipo']}")
    print(f"Cantidad: {row['Cantidad']}")
    print(f"Ultimo precio: {row['Ultimo Precio']}")

    if row['Tipo'] in ('bono', 'obligacion_negociable', 'letra'):
        return row['Cantidad'] * (row['Ultimo Precio'] / 100)
    return row['Cantidad'] * row['Ultimo Precio']

def structure_portfolio(html_data):
    """
    Structure the raw portfolio data and save it in a csv file
    on silver layer.

    Side effects:
    - Save the structured portfolio data in a csv file
    """
    df_iol_portfolio = DataFrame(html_data)

    df_iol_portfolio = df_iol_portfolio[
        ["Activo", "Cantidad", "Último precio", "Precio promedio  de compra"]
    ]

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

    df_iol_portfolio['Tipo'] = df_iol_portfolio['Nombre'].apply(define_type_security)

    df_iol_portfolio['Total'] = df_iol_portfolio.apply(calculate_total, axis=1)


    df_iol_portfolio = df_iol_portfolio[[
        'Ticket', 'Nombre', 'Cantidad', 
        'Ultimo Precio', 'PPC', 'Total', 'Tipo'
        ]]

    return df_iol_portfolio
