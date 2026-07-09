from scripts.pipelines.portfolio_visualization.execute_evolucion_patrimonio import EvolucionHistoricaPatrimonio
import pandas as pd
p = EvolucionHistoricaPatrimonio()
transactions_df = pd.read_csv(p.path_cuentas_unificadas, parse_dates=['Operado'])
portfolio = {'AL30': 0.0}
processed_numeros_paridad = set()
for _, row in transactions_df[transactions_df['Especie'] == 'AL30'].iterrows():
    comp = row['Comprobante']
    cantidad = row['Cantidad']
    numero = row['Numero']
    if pd.notna(comp) and 'PARIDAD' in comp:
        if numero not in processed_numeros_paridad:
            portfolio['AL30'] += round(cantidad, 2)
            processed_numeros_paridad.add(numero)
        if portfolio['AL30'] < 0:
            print(f"Floored to 0! Previous: {portfolio['AL30'] - cantidad}, Row: {comp} {cantidad}")
            portfolio['AL30'] = 0.0
    else:
        portfolio['AL30'] += round(cantidad, 2)
        if portfolio['AL30'] < 0:
             print(f"Floored in normal? No, normal doesn't floor. Balance is {portfolio['AL30']}")
print(f"Final balance: {portfolio['AL30']}")
