import json
import os

def modify_notebook():
    notebook_path = r'c:\Users\tomas\white_finance\notebooks\comparacion_portfolios_merval_s&p500.ipynb'
    
    if not os.path.exists(notebook_path):
        print(f"Error: No se encontró el notebook en {notebook_path}")
        return

    with open(notebook_path, 'r', encoding='utf-8') as f:
        nb = json.load(f)

    # Identificar la celda por su ID 'd2dc5242'
    target_cell_id = 'd2dc5242'
    
    modified = False
    for cell in nb.get('cells', []):
        if cell.get('id') == target_cell_id:
            cell['source'] = [
                "import pandas as pd\n",
                "import numpy as np\n",
                "\n",
                "def calcular_flujos_caja(file_path, output_path):\n",
                "    # 1. Cargar los datos\n",
                "    df = pd.read_csv(file_path)\n",
                "    df['Operado'] = pd.to_datetime(df['Operado'])\n",
                "\n",
                "    # 2. Definir los comprobantes que son retiros o ingresos reales de dinero\n",
                "    comprobantes_flujo_externo = [\n",
                "        'RECIBO DE COBRO', \n",
                "        'REC COBRO DOLARES', \n",
                "        'ORDEN DE PAGO', \n",
                "        'ORD PAGO DOLARES'\n",
                "    ]\n",
                "\n",
                "    # Determinamos el rango histórico completo basado en el CSV (desde la mínima hasta la máxima)\n",
                "    min_date = df['Operado'].min()\n",
                "    max_date = df['Operado'].max()\n",
                "    date_range = pd.date_range(start=min_date, end=max_date, freq='D')\n",
                "    df_range = pd.DataFrame({'Operado': date_range})\n",
                "\n",
                "    # Filtrar el dataframe para flujos externos\n",
                "    df_flujos = df[df['Comprobante'].isin(comprobantes_flujo_externo)].copy()\n",
                "\n",
                "    # 3. Agrupar y sumarizar por día\n",
                "    resumen_diario = df_flujos.groupby('Operado')['Importe'].sum().reset_index()\n",
                "    resumen_diario.rename(columns={'Importe': 'Flujo_Caja_Neto_Diario'}, inplace=True)\n",
                "\n",
                "    # 4. Unir con el rango completo de fechas y rellenar con 0\n",
                "    resumen_flujos = pd.merge(df_range, resumen_diario, on='Operado', how='left').fillna(0)\n",
                "\n",
                "    # 5. Calcular el \"arrastre\" del capital (Flujo Neto Acumulado)\n",
                "    resumen_flujos['Flujo_Caja_Neto'] = resumen_flujos['Flujo_Caja_Neto_Diario'].cumsum()\n",
                "\n",
                "    # 6. Exportar a un nuevo CSV\n",
                "    resumen_flujos.to_csv(output_path, index=False)\n",
                "    \n",
                "    print(f\"Archivo generado con éxito en: {output_path}\")\n",
                "    return resumen_flujos\n",
                "\n",
                "# Ejecución\n",
                "file_name = '../data/analytics/cuentas_unificadas_usd_sorted.csv'\n",
                "output_name = '../data/analytics/portfolio_visualization_data/resumen_flujos_caja.csv'\n",
                "\n",
                "df_resultado = calcular_flujos_caja(file_name, output_name)\n",
                "print(df_resultado.head())\n"
            ]
            modified = True
            break
    
    if modified:
        with open(notebook_path, 'w', encoding='utf-8') as f:
            json.dump(nb, f, indent=1, ensure_ascii=False)
            f.write('\n')
        print("Notebook modificado exitosamente.")
    else:
        print("No se encontró la celda con el ID especificado.")

if __name__ == "__main__":
    modify_notebook()
