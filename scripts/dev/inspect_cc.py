import os
import glob
import pandas as pd

for f in glob.glob('data/balanz/*/Cuenta Corriente/cuenta_corriente_historico.csv'):
    df = pd.read_csv(f, sep='|')
    client = os.path.basename(os.path.dirname(os.path.dirname(f)))
    df_rev = df.iloc[::-1].copy()
    print(f"=== {client} ===")
    desc_col = [c for c in df.columns if 'Descripc' in c][0]
    for _, r in df_rev.iterrows():
        desc = str(r[desc_col])
        if desc.startswith("Recibo de Cobro") or desc.startswith("Comprobante de Pago"):
            print(f"  {r['Fecha']} | {desc} | {r.get('moneda', '')} | Importe: {r.get('Importe', '')} | Saldo: {r.get('Saldo', '')}")
