import pandas as pd
from scripts.pipelines.portfolio_visualization.execute_evolucion_patrimonio import EvolucionHistoricaPatrimonio

p = EvolucionHistoricaPatrimonio()
df = pd.read_csv(p.path_cuentas_unificadas, parse_dates=['Operado'])
df = df.sort_values(by=['Operado', 'Numero']).reset_index(drop=True)
portfolio = {'AL30': 0.0}
processed = set()

for _, row in df[df['Especie'] == 'AL30'].iterrows():
    comp = row['Comprobante']
    cantidad = row['Cantidad']
    num = row['Numero']
    
    if pd.notna(comp) and 'PARIDAD' in comp:
        if num not in processed:
            portfolio['AL30'] += round(cantidad, 2)
            processed.add(num)
    else:
        portfolio['AL30'] += round(cantidad, 2)
        
    if portfolio['AL30'] < 0:
        portfolio['AL30'] = 0.0
        
    print(f"{row['Operado'].date()} | {comp} | {cantidad} -> Balance: {portfolio['AL30']}")
