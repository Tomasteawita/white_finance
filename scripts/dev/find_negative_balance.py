import pandas as pd
import os

path = "/home/jovyan/data/cuenta_corriente_historico.csv"

if not os.path.exists(path):
    print(f"No existe {path}")
else:
    df = pd.read_csv(path)
    
    col_saldo = [c for c in df.columns if 'Saldo' in c][0]
        
    def parse_saldo(val):
        if pd.isna(val):
            return 0.0
        val_str = str(val).strip().replace(',', '')
        if val_str.endswith('-'):
            return -float(val_str[:-1])
        try:
            return float(val_str)
        except:
            return 0.0
            
    df['Saldo_num'] = df[col_saldo].apply(parse_saldo)
    
    negativos = df[df['Saldo_num'] < 0]
    
    print(f"Total registros con saldo negativo: {len(negativos)}")
    if not negativos.empty:
        out_path = "/home/jovyan/data/saldos_negativos.csv"
        # Mostrar algunas columnas relevantes para no saturar consola
        to_save = negativos[[c for c in df.columns if c != 'Saldo_num']]
        to_save.to_csv(out_path, index=False)
        print(f"Los registros fueron guardados en {out_path} para que puedas explorarlos con comodidad.")
