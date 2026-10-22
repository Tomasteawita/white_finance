import os
import glob
import pandas as pd

expected_cols = [
    'Fecha', 
    'Cash_ARS', 
    'Cash_MEP', 
    'Cash_CCL', 
    'Patrimonio_ARS', 
    'Patrimonio_USD', 
    'Patrimonio ahorrado'
]

print("=== VERIFICACIÓN DE REPORTES DE EVOLUCIÓN ===")
for rep_file in sorted(glob.glob('data/balanz/*/reports/evolucion_*.csv')):
    client = os.path.basename(os.path.dirname(os.path.dirname(rep_file)))
    df = pd.read_csv(rep_file)
    cols = list(df.columns)
    assert cols == expected_cols, f"Mismatch in {client}: {cols} != {expected_cols}"
    print(f"\nCliente: {client}")
    print(f"  Columnas: {cols}")
    print(f"  Filas: {len(df)}")
    print(f"  Fecha Inicial: {df['Fecha'].iloc[0]} | Patrimonio ahorrado Inicial: ${df['Patrimonio ahorrado'].iloc[0]:,.2f}")
    print(f"  Fecha Final:   {df['Fecha'].iloc[-1]} | Patrimonio ahorrado Final:   ${df['Patrimonio ahorrado'].iloc[-1]:,.2f}")
    print(f"  Patrimonio_ARS Final: ${df['Patrimonio_ARS'].iloc[-1]:,.2f} | Patrimonio_USD Final: ${df['Patrimonio_USD'].iloc[-1]:,.2f}")

print("\n✅ Todos los reportes validados exitosamente con la estructura requerida.")
