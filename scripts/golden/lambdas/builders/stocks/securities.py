from pandas import read_csv

def get_securitie_type(name):
    name = name.lower().split(' ')
    keywords = {
        'cedear': 'cedear',
        'on': 'obligacion negociable',
        'obligacion': 'obligacion negociable',
        'bon': 'bono',
        'vto': 'bono',
        'letra': 'letra del tesoro',
        'fondo': 'fondo comun de inversion',
        'fci': 'fondo comun de inversion',
        'comun': 'fondo comun de inversion'
    }

    for key, value in keywords.items():
        if key in name:
            return value
    return 'accion'

def get_financial_instrument_type(securitie_type):
    securitie_type = securitie_type.lower()

    if securitie_type in ['cedear', 'accion', 'fondo comun de inversion']:
        return 'renta variable'
    elif securitie_type in ['bono', 'obligacion negociable', 'letra del tesoro']:
        return 'renta fija'
    else:
        return 'desconocido'

# def load_securities(df):
#     """Load dataframe in golden.securities table"""

#     # Crear una conexión a la base de datos
#     conn = connect(
#         host='database-1.c0zj6jw4d6zg.us-east-1.rds.amazonaws.com',
#         port=3306,
#         user

def securities(event):
    # Leer con pandas un archivo csv
    df = read_csv('../data/silver/iol_portfolio_2024-09-29.csv', sep=',')

    # convierto todas las columnas en minusculas
    df.columns = df.columns.str.lower()

    df['securitie_type'] = df['nombre'].apply(get_securitie_type)
    df['financial_instrument_type'] = df['securitie_type'].apply(get_financial_instrument_type)

    # cambio el nombre de la columna 'nombre' por 'full_name'
    df.rename(columns={'nombre': 'full_name'}, inplace=True)

    # elimino la columna 'cantidad', 'ultimo precio', 'ppc', 'total'
    df.drop(columns=['cantidad', 'ultimo precio', 'ppc', 'total'], inplace=True)

    # elimino la columna 'tipo' si existe
    if 'tipo' in df.columns:
        df.drop(columns=['tipo'], inplace=True)

    # creo una columna llamada 'dividend_yield' y 'par_value' con valores nulos
    df['dividend_yield'] = None
    df['par_value'] = None
    df['full_name'] = df['full_name'].str.upper()



    load_securities(df)


def lambda_handler(event, context):
    """
    Lambda function that structures the raw portfolio data and saves it
    in a csv file on silver layer.
    """
    transform, broker = set_transform(event)
    structure_bronze_data_to_silver_template(transform)(event, broker)
    return {
        'statusCode': 200,
        'body': json.dumps('Portfolio structured successfully')
    }