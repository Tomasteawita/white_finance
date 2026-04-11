import pandas as pd
from typing import List

def get_tickers_by_range(df: pd.DataFrame, start_idx: int, end_idx: int) -> List[str]:
    """
    Extrae una lista de tickers desde un DataFrame dentro de un rango de filas específico.

    Lógica de Negocio:
    Para optimizar el procesamiento de screening o análisis por lotes (batch processing), 
    es necesario segmentar el universo de inversión. Esta función permite extraer 
    subconjuntos de activos (tickers) para evitar la sobrecarga de APIs o recursos 
    al procesar el S&P500 completo.

    Args:
        df (pd.DataFrame): DataFrame que contiene al menos la columna 'ticker'.
        start_idx (int): Índice inicial del rango (inclusivo).
        end_idx (int): Índice final del rango (exclusivo, siguiendo convención iloc).

    Returns:
        List[str]: Lista de tickers encontrados en el rango solicitado.
    """
    try:
        # Validación defensiva de la columna requerida
        if 'ticker' not in df.columns:
            raise KeyError("El DataFrame no contiene una columna llamada 'ticker'.")

        # Selección del rango y conversión a lista
        # Se utiliza iloc para asegurar que la selección sea por posición entera
        tickers_list = df.iloc[start_idx:end_idx]['ticker'].tolist()
        
        return tickers_list

    except Exception as e:
        print(f"Error al extraer tickers: {e}")
        return []

# Ejemplo de uso (opcional, para validación manual)
if __name__ == "__main__":
    # Simulación de carga desde el notebook mencionado
    # df = pd.read_csv('../data/oportunidades_de_inversion/listado de empresas del S&P500.csv', sep=';')
    # print(get_tickers_by_range(df, 0, 5))
    pass
