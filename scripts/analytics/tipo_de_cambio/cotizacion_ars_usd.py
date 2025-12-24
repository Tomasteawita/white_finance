import yfinance as yf
from datetime import datetime

# Definir el período de tiempo
def main():
    start_date = "2024-01-01"
    end_date = datetime.today().strftime('%Y-%m-%d')

    # Descargar datos del ADR (GGAL en NYSE)
    ggal_adr = yf.download("GGAL", start=start_date, end=end_date)

    # Descargar datos de la acción en pesos (GGAL en BCBA)
    ggal_pesos = yf.download("GGAL.BA", start=start_date, end=end_date)

    # Mostrar las primeras filas de cada dataset
    print("ADR GGAL (USD):")
    print(ggal_adr.head())
    print("\nAcción GGAL en Pesos (ARS):")
    print(ggal_pesos.head())}# Hacer join entre ambos dataframes y quedarse solo con Date y Close
    ggal_combined = ggal_adr[['Close']].join(ggal_pesos[['Close']], how='inner', lsuffix='_adr', rsuffix='_pesos')

    # Renombrar las columnas para mayor claridad
    ggal_combined.columns = ['Close_ADR_USD', 'Close_GGAL_ARS']
    # Genero la columna tipo de cambio, multiplicando Close_GGAL_ARS * 10 y luego divido por Close_ADR_USD
    ggal_combined['Tipo_Cambio_ARS_USD'] = (ggal_combined['Close_GGAL_ARS'] * 10) / ggal_combined['Close_ADR_USD']  
    ggal_combined.to_csv("../data/tipo_cambio_ggal.csv", index=True)

if __name__ == "__main__":
    main()