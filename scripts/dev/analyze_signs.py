import pandas as pd
import os
import json

base_dir = "/home/jovyan/data"

archivos = {
    "DOLARES CABLE": "cuenta_corriente_dolares_cable_historico.csv",
    "DOLARES MEP": "cuenta_corriente_dolares_historico.csv",
    "PESOS": "cuenta_corriente_historico.csv"
}

resultados = {}

def parse_importe(val):
    if pd.isna(val):
        return 0.0
    val_str = str(val).strip().replace(',', '')
    if val_str.endswith('-'):
        return -float(val_str[:-1])
    try:
        return float(val_str)
    except:
        return 0.0

for moneda, archivo in archivos.items():
    ruta = os.path.join(base_dir, archivo)
    if not os.path.exists(ruta):
        print(f"No existe {ruta}")
        continue
        
    df = pd.read_csv(ruta)
    
    col_comp = [c for c in df.columns if 'Comprobante' in c]
    col_imp = [c for c in df.columns if 'Importe' in c]
    
    if not col_comp or not col_imp:
        print(f"Faltan columnas en {archivo}")
        continue
        
    col_comp = col_comp[0]
    col_imp = col_imp[0]
    
    # Parsear importe a float
    df['Importe_Float'] = df[col_imp].apply(parse_importe)
    
    # Agrupar por comprobante y ver min/max para determinar el signo
    res_moneda = {}
    for comp, group in df.groupby(col_comp):
        comp_str = str(comp).strip()
        max_val = group['Importe_Float'].max()
        min_val = group['Importe_Float'].min()
        
        if max_val > 0 and min_val < 0:
            sign = "MIXTO"
        elif min_val < 0:
            sign = "NEGATIVO"
        elif max_val > 0:
            sign = "POSITIVO"
        else:
            sign = "CERO"
            
        res_moneda[comp_str] = sign
        
    resultados[moneda] = res_moneda

with open("/home/jovyan/work/signos_comprobantes.json", "w", encoding="utf-8") as f:
    json.dump(resultados, f, indent=4, ensure_ascii=False)

print("Análisis de signos completado. Resultados guardados en signos_comprobantes.json")
