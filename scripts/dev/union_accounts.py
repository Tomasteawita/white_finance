import pandas as pd
import os

base_dir = "/home/jovyan/data"
cuentas_dir = os.path.join(base_dir, "cuentas_corrientes")

archivos = [
    {
        "moneda": "DOLARES CABLE",
        "previa": os.path.join(cuentas_dir, "clean_cuenta corriente historico dolares cable.csv"),
        "actual": os.path.join(base_dir, "cuenta_corriente_dolares_cable_historico.csv"),
        "salida": os.path.join(base_dir, "historico_completo_dolares_cable.csv")
    },
    {
        "moneda": "DOLARES MEP",
        "previa": os.path.join(cuentas_dir, "clean_cuenta corriente historico dolar mep.csv"),
        "actual": os.path.join(base_dir, "cuenta_corriente_dolares_historico.csv"),
        "salida": os.path.join(base_dir, "historico_completo_dolares_mep.csv")
    },
    {
        "moneda": "PESOS",
        "previa": os.path.join(cuentas_dir, "clean_Cuenta corriente historico en pesos.csv"),
        "actual": os.path.join(base_dir, "cuenta_corriente_historico.csv"),
        "salida": os.path.join(base_dir, "historico_completo_pesos.csv")
    }
]

for p in archivos:
    if os.path.exists(p["previa"]) and os.path.exists(p["actual"]):
        df_prev = pd.read_csv(p["previa"])
        df_act = pd.read_csv(p["actual"])
        
        # Eliminar registros sin número de comprobante válido para que el filtrado sea robusto
        df_prev = df_prev.dropna(subset=['Numero']).copy()
        df_act = df_act.dropna(subset=['Numero']).copy()
        
        # Castear 'Numero' a numérico para evitar falsos negativos por '1234' vs '1234.0'
        df_prev['Numero'] = pd.to_numeric(df_prev['Numero'], errors='coerce')
        df_act['Numero'] = pd.to_numeric(df_act['Numero'], errors='coerce')
        
        # Filtrar previas: mantener los que NO están en "Numero" de actuales
        numeros_actuales = set(df_act['Numero'].dropna())
        df_prev_filtrado = df_prev[~df_prev['Numero'].isin(numeros_actuales)]
        
        print(f"--- {p['moneda']} ---")
        print(f"Registros en tabla actual   : {len(df_act)}")
        print(f"Registros en tabla previa   : {len(df_prev)}")
        print(f"A añadir (previa no en act.): {len(df_prev_filtrado)}\n")
        
        # Unión secuencial (antiguos primero, nuevos después)
        df_final = pd.concat([df_prev_filtrado, df_act], ignore_index=True)
        
        # Ordenar por fecha 'Liquida' para que quede 100% cronológico
        # Convertimos local a datetime para asegurar un sort correcto de strings tipo 'YY-MM-DD'
        df_final['Liquida_Sort'] = pd.to_datetime(df_final['Liquida'], errors='coerce')
        df_final = df_final.sort_values(by=['Liquida_Sort'])
        df_final.drop(columns=['Liquida_Sort'], inplace=True)
        
        # Exportar a la ruta final
        df_final.to_csv(p["salida"], index=False)
        print(f"Histórico completo guardado en {p['salida']} con {len(df_final)} filas.\n")
    else:
        print(f"Archivos faltantes para {p['moneda']}")
