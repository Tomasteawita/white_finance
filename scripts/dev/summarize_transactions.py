import pandas as pd
import os

path = r'c:\Users\tomas\white_finance\data\analytics\cuentas_unificadas_sorted.csv'
df = pd.read_csv(path, parse_dates=['Operado'])
df = df[df['Operado'] >= '2026-06-01']

print(f"Total registros desde 2026-06-01: {len(df)}")
print("\nResumen por Comprobante:")
print(df['Comprobante'].value_counts())

print("\nResumen por Origen:")
print(df['Origen'].value_counts())

print("\nResumen por Especie:")
print(df['Especie'].value_counts())

print("\nOperaciones relevantes de COMPRA/VENTA/SUSCRIPCION/RESCATE o similares:")
relevantes = df[df['Comprobante'].str.contains('COMPRA|VENTA|SUSCRIP|RESCAT|RECIBO|ORDEN', case=False, na=False)]

grouped = relevantes.groupby(['Operado', 'Comprobante', 'Especie', 'Origen']).agg({
    'Cantidad': 'sum',
    'Importe': 'sum',
    'Saldo': 'last'
}).reset_index()

print("\nDetalle cronológico de operaciones relevantes:")
pd.set_option('display.max_rows', None)
print(grouped.to_string(index=False))

