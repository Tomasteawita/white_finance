import pandas as pd

def main():
    # Cargar los datos desde el archivo CSV
    df = pd.read_csv('data/raw/tipo_de_cambio/cuenta_corriente_ars_a_usd.csv')

    # Convertir la columna 'fecha' a tipo datetime
    df['fecha'] = pd.to_datetime(df['fecha'], format='%Y-%m-%d')

    # Ordenar el DataFrame por fecha
    df = df.sort_values(by='fecha')

    # Guardar el DataFrame procesado en un nuevo archivo CSV
    df.to_csv('data/processed/tipo_de_cambio/cuenta_corriente_ars_a_usd.csv', index=False)

if __name__ == "__main__":
    main()