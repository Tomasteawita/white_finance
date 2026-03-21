import pandas as pd
import os

base_dir = "/home/jovyan/data"
cuentas_dir = os.path.join(base_dir, "cuentas_corrientes")

archivos = [
    {
        "moneda": "DOLARES CABLE",
        "previa": os.path.join(cuentas_dir, "clean_cuenta corriente historico dolares cable.csv"),
        "actual": os.path.join(base_dir, "cuenta_corriente_dolares_cable_historico.csv")
    },
    {
        "moneda": "DOLARES MEP",
        "previa": os.path.join(cuentas_dir, "clean_cuenta corriente historico dolar mep.csv"),
        "actual": os.path.join(base_dir, "cuenta_corriente_dolares_historico.csv")
    },
    {
        "moneda": "PESOS",
        "previa": os.path.join(cuentas_dir, "clean_Cuenta corriente historico en pesos.csv"),
        "actual": os.path.join(base_dir, "cuenta_corriente_historico.csv")
    }
]

for p in archivos:
    if os.path.exists(p["previa"]) and os.path.exists(p["actual"]):
        df_prev = pd.read_csv(p["previa"], nrows=0)
        df_act = pd.read_csv(p["actual"], nrows=0)
        
        cols_prev = set(df_prev.columns)
        cols_act = set(df_act.columns)
        
        print(f"--- {p['moneda']} ---")
        if cols_prev == cols_act:
            if list(df_prev.columns) == list(df_act.columns):
                print("Las columnas coinciden exactamente y en las mismas posiciones.")
            else:
                print("Las columnas coinciden exactamente pero difieren en el orden.")
        else:
            print("Diferencias encontradas:")
            print(f"  En previa pero no en actual: {cols_prev - cols_act}")
            print(f"  En actual pero no en previa: {cols_act - cols_prev}")
            print(f"  Orden Previa: {list(df_prev.columns)}")
            print(f"  Orden Actual: {list(df_act.columns)}")
        print("\n")
    else:
        print(f"Archivos faltantes para {p['moneda']}. Previa: {os.path.exists(p['previa'])}, Actual: {os.path.exists(p['actual'])}")
