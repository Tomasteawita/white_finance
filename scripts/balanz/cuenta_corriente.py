import pandas as pd


class CuentaCorriente:







if __name__ == "__main__":
    # Cargar datos desde un archivo CSV
    df = pd.read_csv("data/cuenta_corriente_historico.csv")

    # Mostrar las primeras filas del DataFrame
    print(df.head())

    # Realizar análisis básico
    resumen = df.describe()
    print(resumen)

    # Guardar el resumen en un nuevo