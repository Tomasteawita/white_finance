import pandas as pd
import subprocess

def cuenta_corriente_ars_a_usd(conversion_type="ars_to_usd"):
    """
    Procesa la cuenta corriente según el tipo de conversión especificado.
    
    Args:
        conversion_type (str): Tipo de conversión a realizar
            - "ars_to_usd": Convierte cuenta corriente en pesos a dólares usando tipo de cambio GGAL
            - "mep_to_ccl": Convierte cuenta corriente en dólares MEP a dólares CCL usando ratio MEP/CCL
    """
    
    if conversion_type == "ars_to_usd":
        # Cargar los datos de cuenta corriente histórica en pesos
        print("Cargando datos de cuenta corriente histórica en pesos...")
        df_cuenta = pd.read_csv('../data/cuenta_corriente_historico.csv')
        
        # Cargar los datos de tipo de cambio GGAL
        print("Cargando datos de tipo de cambio GGAL...")
        df_tipo_cambio = pd.read_csv('../data/tipo_cambio_ggal.csv')
        
        # Convertir la columna 'Operado' a tipo datetime
        df_cuenta['Operado'] = pd.to_datetime(df_cuenta['Operado'], format='%Y-%m-%d')
        
        # Convertir la columna 'Date' del tipo de cambio a datetime
        df_tipo_cambio['Date'] = pd.to_datetime(df_tipo_cambio['Date'], format='%Y-%m-%d')
        
        # Realizar el merge usando la fecha de liquidación como clave
        df_merged = pd.merge(
            df_cuenta,
            df_tipo_cambio[['Date', 'Tipo_Cambio_ARS_USD']],
            left_on='Operado',
            right_on='Date',
            how='left'
        )
        
        # Eliminar la columna 'Date' duplicada del merge
        df_merged = df_merged.drop(columns=['Date'])
        
        # Forward fill para rellenar valores faltantes de tipo de cambio
        # (usa el último tipo de cambio disponible)
        df_merged['Tipo_Cambio_ARS_USD'] = df_merged['Tipo_Cambio_ARS_USD'].fillna(method='ffill')
        
        # Backward fill para los primeros registros si no tienen tipo de cambio
        df_merged['Tipo_Cambio_ARS_USD'] = df_merged['Tipo_Cambio_ARS_USD'].fillna(method='bfill')
        
        # Calcular los importes en dólares
        df_merged['Importe_USD'] = df_merged['Importe'] / df_merged['Tipo_Cambio_ARS_USD']
        df_merged['Saldo_USD'] = df_merged['Saldo'] / df_merged['Tipo_Cambio_ARS_USD']
        
        # Redondear a 2 decimales para mejor legibilidad
        df_merged['Importe_USD'] = df_merged['Importe_USD'].round(2)
        df_merged['Saldo_USD'] = df_merged['Saldo_USD'].round(2)
        df_merged['Tipo_Cambio_ARS_USD'] = df_merged['Tipo_Cambio_ARS_USD'].round(4)
        
        # Guardar el DataFrame procesado en un nuevo archivo CSV
        output_file = '../data/cuenta_corriente_pesos_dolarizada_historico.csv'
        df_merged.to_csv(output_file, index=False)
        
        print(f"Proceso completado. Se procesaron {len(df_merged)} registros.")
        print(f"Rango de fechas: {df_merged['Operado'].min()} a {df_merged['Operado'].max()}")
        print(f"Registros con tipo de cambio: {df_merged['Tipo_Cambio_ARS_USD'].notna().sum()}")
        print(f"Archivo guardado: {output_file}")
    
    elif conversion_type == "mep_to_ccl":
        # Paso 1: Calcular el ratio MEP/CCL
        print("Cargando datos de tipo de cambio GGAL CCL...")
        df_ccl = pd.read_csv('../data/tipo_cambio_ggal.csv')
        
        print("Cargando datos de tipo de cambio GGAL MEP...")
        df_mep = pd.read_csv('../data/tipo_cambio_ggal_mep.csv')
        
        # Convertir columnas Date a datetime
        df_ccl['Date'] = pd.to_datetime(df_ccl['Date'], format='%Y-%m-%d')
        df_mep['Date'] = pd.to_datetime(df_mep['Date'], format='%Y-%m-%d')
        
        # Inner join entre los dos archivos por Date
        print("Calculando ratio MEP/CCL...")
        df_ratio = pd.merge(
            df_mep[['Date', 'Tipo_Cambio_ARS_USD']],
            df_ccl[['Date', 'Tipo_Cambio_ARS_USD']],
            on='Date',
            how='inner',
            suffixes=('_mep', '_ccl')
        )
        
        # Calcular el ratio: MEP / CCL
        df_ratio['ratio_mep_ccl'] = df_ratio['Tipo_Cambio_ARS_USD_mep'] / df_ratio['Tipo_Cambio_ARS_USD_ccl']
        
        # Paso 2: Leer cuenta corriente en dólares
        print("Cargando datos de cuenta corriente en dólares...")
        df_cuenta = pd.read_csv('../data/cuenta_corriente_dolares_historico.csv')
        
        # Convertir la columna 'Operado' a tipo datetime
        df_cuenta['Operado'] = pd.to_datetime(df_cuenta['Operado'], format='%Y-%m-%d')
        
        # Paso 3: Left join entre cuenta corriente y ratio
        print("Aplicando ratio MEP/CCL a cuenta corriente...")
        df_merged = pd.merge(
            df_cuenta,
            df_ratio[['Date', 'ratio_mep_ccl']],
            left_on='Operado',
            right_on='Date',
            how='left'
        )
        
        # Eliminar la columna 'Date' duplicada del merge
        df_merged = df_merged.drop(columns=['Date'])
        
        # Forward fill para rellenar valores faltantes de ratio
        df_merged['ratio_mep_ccl'] = df_merged['ratio_mep_ccl'].fillna(method='ffill')
        
        # Backward fill para los primeros registros si no tienen ratio
        df_merged['ratio_mep_ccl'] = df_merged['ratio_mep_ccl'].fillna(method='bfill')
        
        # Paso 4: Multiplicar Importe y Saldo por el ratio
        df_merged['Importe_USD_CCL'] = df_merged['Importe'] * df_merged['ratio_mep_ccl']
        df_merged['Saldo_USD_CCL'] = df_merged['Saldo'] * df_merged['ratio_mep_ccl']
        
        # Redondear a 2 decimales para mejor legibilidad
        df_merged['Importe_USD_CCL'] = df_merged['Importe_USD_CCL'].round(2)
        df_merged['Saldo_USD_CCL'] = df_merged['Saldo_USD_CCL'].round(2)
        df_merged['ratio_mep_ccl'] = df_merged['ratio_mep_ccl'].round(6)
        
        # Paso 5: Guardar el DataFrame procesado
        output_file = '../data/cuenta_corriente_mep_ccl_historico.csv'
        df_merged.to_csv(output_file, index=False)
        
        print(f"Proceso completado. Se procesaron {len(df_merged)} registros.")
        print(f"Rango de fechas: {df_merged['Operado'].min()} a {df_merged['Operado'].max()}")
        print(f"Registros con ratio: {df_merged['ratio_mep_ccl'].notna().sum()}")
        print(f"Archivo guardado: {output_file}")
    
    else:
        raise ValueError(f"Tipo de conversión no válido: {conversion_type}. Use 'ars_to_usd' o 'mep_to_ccl'")

if __name__ == "__main__":
    # Por defecto ejecuta la conversión ARS a USD
    # Para ejecutar MEP a CCL, usa: cuenta_corriente_ars_a_usd("mep_to_ccl")
    cuenta_corriente_ars_a_usd()