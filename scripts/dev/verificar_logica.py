import pandas as pd
import numpy as np
import os

def test_logic():
    file_path = r'c:\Users\tomas\white_finance\data\analytics\cuentas_unificadas_usd_sorted.csv'
    output_path = r'c:\Users\tomas\white_finance\data\analytics\portfolio_visualization_data\resumen_flujos_caja_test.csv'
    
    if not os.path.exists(file_path):
        print(f"Error: No se encontró el CSV en {file_path}")
        return

    # 1. Cargar los datos
    df = pd.read_csv(file_path)
    df['Operado'] = pd.to_datetime(df['Operado'])

    # 2. Definir los comprobantes que son retiros o ingresos reales de dinero
    comprobantes_flujo_externo = [
        'RECIBO DE COBRO', 
        'REC COBRO DOLARES', 
        'ORDEN DE PAGO', 
        'ORD PAGO DOLARES'
    ]

    # Determinamos el rango histórico completo basado en el CSV (desde la mínima hasta la máxima)
    min_date = df['Operado'].min()
    max_date = df['Operado'].max()
    print(f"Min date: {min_date}, Max date: {max_date}")
    
    date_range = pd.date_range(start=min_date, end=max_date, freq='D')
    df_range = pd.DataFrame({'Operado': date_range})

    # Filtrar el dataframe para flujos externos
    df_flujos = df[df['Comprobante'].isin(comprobantes_flujo_externo)].copy()

    # 3. Agrupar y sumarizar por día
    resumen_diario = df_flujos.groupby('Operado')['Importe'].sum().reset_index()
    resumen_diario.rename(columns={'Importe': 'Flujo_Caja_Neto_Diario'}, inplace=True)

    # 4. Unir con el rango completo de fechas y rellenar con 0
    resumen_flujos = pd.merge(df_range, resumen_diario, on='Operado', how='left').fillna(0)

    # 5. Calcular el "arrastre" del capital (Flujo Neto Acumulado)
    resumen_flujos['Flujo_Caja_Neto'] = resumen_flujos['Flujo_Caja_Neto_Diario'].cumsum()

    # 6. Exportar a un nuevo CSV
    resumen_flujos.to_csv(output_path, index=False)
    
    print(f"Archivo generado con éxito en: {output_path}")
    print(f"Total rows: {len(resumen_flujos)}")
    print(f"First 5 rows:\n{resumen_flujos.head()}")
    print(f"Last 5 rows:\n{resumen_flujos.tail()}")
    
    # Verificación de gaps
    if len(resumen_flujos) == (max_date - min_date).days + 1:
        print("Verificación de continuidad: OK (No hay huecos)")
    else:
        print(f"Verificación de continuidad: FALLIDA (Esperado {(max_date - min_date).days + 1}, obtenido {len(resumen_flujos)})")

if __name__ == "__main__":
    test_logic()
