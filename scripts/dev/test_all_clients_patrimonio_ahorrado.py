import os
import glob
import pandas as pd
import numpy as np
from sqlalchemy import create_engine

db_uri = "postgresql://postgres:postgres@localhost:5432/postgres"
engine = create_engine(db_uri)
df_ccl = pd.read_sql("SELECT date, ccl FROM earnings.ccl_mep", engine)
df_ccl['date'] = pd.to_datetime(df_ccl['date'])
ccl_series = df_ccl.drop_duplicates(subset=['date']).set_index('date')['ccl']

for client_dir in sorted(glob.glob('data/balanz/*')):
    if not os.path.isdir(client_dir):
        continue
    client = os.path.basename(client_dir)
    cc_path = os.path.join(client_dir, 'Cuenta Corriente', 'cuenta_corriente_historico.csv')
    if not os.path.exists(cc_path):
        continue
    
    df_cc = pd.read_csv(cc_path, sep='|')
    desc_col = [c for c in df_cc.columns if 'Descripc' in c][0]
    df_cc = df_cc.rename(columns={desc_col: 'Descripcion'})
    df_cc['Fecha'] = pd.to_datetime(df_cc['Fecha'])
    
    # Sort chronologically
    df_cc = df_cc.iloc[::-1].sort_values(by='Fecha', ascending=True, kind='mergesort').reset_index(drop=True)
    
    # Calculate flows
    flows = []
    for _, row in df_cc.iterrows():
        fecha = row['Fecha']
        desc = str(row['Descripcion']).strip()
        importe = float(row['Importe']) if pd.notna(row['Importe']) else 0.0
        moneda_str = str(row.get('moneda', '')).strip().lower()
        
        is_rc = desc.startswith("Recibo de Cobro")
        is_cp = desc.startswith("Comprobante de Pago")
        
        if is_rc or is_cp:
            val = abs(importe) if is_rc else -abs(importe)
            is_usd = ('lares' in moneda_str or 'lres' in moneda_str or 'd' in moneda_str or '7000' in moneda_str) and ('pesos' not in moneda_str)
            flows.append({
                'Fecha': fecha,
                'Flujo_ARS': 0.0 if is_usd else val,
                'Flujo_USD': val if is_usd else 0.0,
                'Desc': desc,
                'Moneda': moneda_str,
                'Importe': importe
            })
            
    df_flows = pd.DataFrame(flows)
    if df_flows.empty:
        print(f"=== {client}: NO FLOWS ===")
        continue
        
    daily_flows = df_flows.groupby('Fecha')[['Flujo_ARS', 'Flujo_USD']].sum().reset_index()
    
    # Load existing evolucion file to see dates
    rep_file = os.path.join(client_dir, 'reports', f'evolucion_{client.lower()}.csv')
    if os.path.exists(rep_file):
        df_rep = pd.read_csv(rep_file)
        df_rep['Fecha'] = pd.to_datetime(df_rep['Fecha'])
        idx = pd.date_range(df_rep['Fecha'].min(), df_rep['Fecha'].max())
        
        df_aligned = pd.DataFrame(index=idx)
        df_aligned.index.name = 'Fecha'
        daily_flows_indexed = daily_flows.set_index('Fecha')
        
        df_aligned['Flujo_ARS'] = daily_flows_indexed['Flujo_ARS'].reindex(idx, fill_value=0.0)
        df_aligned['Flujo_USD'] = daily_flows_indexed['Flujo_USD'].reindex(idx, fill_value=0.0)
        
        ccl_aligned = ccl_series.reindex(idx).ffill().bfill()
        df_aligned['CCL'] = ccl_aligned
        
        df_aligned['Flujo_Neto_Total_ARS'] = df_aligned['Flujo_ARS'] + (df_aligned['Flujo_USD'] * df_aligned['CCL'])
        df_aligned['Patrimonio ahorrado'] = df_aligned['Flujo_Neto_Total_ARS'].cumsum()
        
        print(f"=== {client} ===")
        print(f"  Start: {idx.min().strftime('%Y-%m-%d')} -> Patrimonio ahorrado: ${df_aligned['Patrimonio ahorrado'].iloc[0]:,.2f}")
        print(f"  End:   {idx.max().strftime('%Y-%m-%d')} -> Patrimonio ahorrado: ${df_aligned['Patrimonio ahorrado'].iloc[-1]:,.2f}")
        print(f"  Total inflows ARS: ${df_flows[df_flows['Flujo_ARS'] > 0]['Flujo_ARS'].sum():,.2f}")
        print(f"  Total inflows USD: ${df_flows[df_flows['Flujo_USD'] > 0]['Flujo_USD'].sum():,.2f}")
