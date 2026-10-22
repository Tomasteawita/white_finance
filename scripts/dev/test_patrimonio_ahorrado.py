import os
import glob
import pandas as pd

for client_dir in glob.glob('data/balanz/*'):
    if not os.path.isdir(client_dir):
        continue
    client = os.path.basename(client_dir)
    cc_path = os.path.join(client_dir, 'Cuenta Corriente', 'cuenta_corriente_historico.csv')
    if not os.path.exists(cc_path):
        continue
    df = pd.read_csv(cc_path, sep='|')
    desc_col = [c for c in df.columns if 'Descripc' in c][0]
    df['Fecha'] = pd.to_datetime(df['Fecha'])
    df_sorted = df.sort_values('Fecha')
    
    first_row = df_sorted.iloc[0]
    first_rc = df_sorted[df_sorted[desc_col].str.startswith('Recibo de Cobro')]
    first_rc_date = first_rc.iloc[0]['Fecha'] if not first_rc.empty else None
    
    print(f"Client: {client}")
    print(f"  First CC date: {first_row['Fecha'].strftime('%Y-%m-%d')} | Desc: {first_row[desc_col]}")
    print(f"  First Recibo Cobro date: {first_rc_date.strftime('%Y-%m-%d') if first_rc_date is not None else 'None'}")
