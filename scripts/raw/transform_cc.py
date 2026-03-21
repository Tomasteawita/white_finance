import pandas as pd
import os
import json

# Paths base dentro de docker que tiene montado localmente el root
data_dir = "/home/jovyan/data"
cuentas_dir = os.path.join(data_dir, "cuentas_corrientes")
notebooks_dir = "/home/jovyan/work"

# Cargar mapeo de comprobantes
with open(os.path.join(notebooks_dir, "mapeo_comprobantes.json"), "r", encoding="utf-8") as f:
    mapeo = json.load(f)

# Cargar mapeo de signos
with open(os.path.join(notebooks_dir, "signos_comprobantes.json"), "r", encoding="utf-8") as f:
    signos = json.load(f)

archivos = {
    "DOLARES CABLE": "cuenta corriente historico dolares cable.csv",
    "DOLARES MEP": "cuenta corriente historico dolar mep.csv",
    "PESOS": "Cuenta corriente historico en pesos.csv"
}

for moneda, archivo in archivos.items():
    ruta = os.path.join(cuentas_dir, archivo)
    if not os.path.exists(ruta):
        print(f"No existe {ruta}")
        continue
        
    df = pd.read_csv(ruta)
    
    # Encontrar columnas
    col_cpbt = [c for c in df.columns if 'Cpbt' in c]
    col_importe = [c for c in df.columns if 'Importe' in c]
    
    if not col_cpbt or not col_importe:
        print(f"Columnas necesarias no encontradas en {archivo}")
        continue
        
    col_cpbt = col_cpbt[0]
    col_importe = col_importe[0]
    
    # Limpiar espacios en la columna para que coincida con las claves del JSON
    df[col_cpbt] = df[col_cpbt].astype(str).str.strip()
    
    # Aplicar mapeo de comprobante
    dict_map = mapeo.get(moneda, {})
    
    def apply_map(val):
        mapped = dict_map.get(val, None)
        return mapped if mapped is not None else val
        
    df[col_cpbt] = df[col_cpbt].apply(apply_map)
    
    # Si es pesos, filtrar los que terminan con "COMERCIO EXTERIOR"
    if moneda == "PESOS":
        df = df[~df[col_cpbt].astype(str).str.endswith("COMERCIO EXTERIOR")]
        
    # LOGICA DE IMPORTES
    # 1. Eliminar '-'
    # 2. Eliminar ','
    # 3. Castear a double (float)
    df[col_importe] = df[col_importe].astype(str).str.replace('-', '', regex=False).str.replace(',', '', regex=False).astype(float)
    
    # 4. Aplicar signo de acuerdo al comprobante mapeado
    dict_signos = signos.get(moneda, {})
    
    def apply_sign(row):
        comp = str(row[col_cpbt]).strip()
        val = row[col_importe]
        signo = dict_signos.get(comp, "POSITIVO")  # Asumimos positivo si no se encuentra
        
        if signo == "NEGATIVO":
            return -abs(val)
        else:
            return abs(val)
            
    df[col_importe] = df.apply(apply_sign, axis=1)

    # Lógica de Saldo
    col_saldo = [c for c in df.columns if 'Saldo' in c]
    if col_saldo:
        c_sal = col_saldo[0]
        def fix_saldo(val):
            if pd.isna(val):
                return 0.0
            s = str(val).strip()
            if s.endswith('-'):
                s = '-' + s[:-1]
            s = s.replace(',', '')
            try:
                return float(s) * -1.0
            except:
                return 0.0
        df[c_sal] = df[c_sal].apply(fix_saldo)

    # Lógica de Referencia/Cantidad/Precio
    col_ref_cant_precio = [c for c in df.columns if c == 'Referencia/Cantidad/Precio']
    if col_ref_cant_precio:
        c_ref = col_ref_cant_precio[0]
        
        def parse_ref(val):
            if pd.isna(val):
                return pd.Series({'Referencia_Clean': '', 'Cantidad': 0.0, 'Precio': 0.0})
            
            s = str(val).strip()
            if not s:
                return pd.Series({'Referencia_Clean': '', 'Cantidad': 0.0, 'Precio': 0.0})
                
            parts = [p for p in s.split(' ') if p.strip()]
            
            all_floats = True
            floats = []
            for p in parts:
                p_clean = p.replace(',', '')
                try:
                    floats.append(float(p_clean))
                except ValueError:
                    all_floats = False
                    break
            
            if all_floats and len(floats) > 0:
                cant = floats[0]
                precio = floats[1] if len(floats) > 1 else 0.0
                return pd.Series({'Referencia_Clean': '', 'Cantidad': cant, 'Precio': precio})
            else:
                return pd.Series({'Referencia_Clean': " ".join(parts), 'Cantidad': 0.0, 'Precio': 0.0})
                
        res = df[c_ref].apply(parse_ref)
        df['Referencia'] = res['Referencia_Clean']
        df['Cantidad'] = res['Cantidad']
        df['Precio'] = res['Precio']

    # Lógica de Especie Cantidad/Referencia (DOLARES)
    col_espcantref = [c for c in df.columns if c == 'Especie Cantidad/Referencia']
    if col_espcantref:
        c_ref2 = col_espcantref[0]
        
        def parse_ref2(val):
            if pd.isna(val) or not str(val).strip():
                return pd.Series({'Especie': '', 'Cantidad': 0.0, 'Referencia_Clean': ''})
            
            s = str(val).strip()
            parts = [p for p in s.split(' ') if p.strip()]
            
            if len(parts) >= 2:
                segundo = parts[1]
                if segundo.endswith('-'):
                    segundo = '-' + segundo[:-1]
                
                segundo_clean = segundo.replace(',', '')
                try:
                    cant = float(segundo_clean)
                    return pd.Series({'Especie': parts[0], 'Cantidad': cant, 'Referencia_Clean': ''})
                except ValueError:
                    pass
                    
            return pd.Series({'Especie': '', 'Cantidad': 0.0, 'Referencia_Clean': " ".join(parts)})

        res2 = df[c_ref2].apply(parse_ref2)
        df['Especie'] = res2['Especie']
        df['Cantidad'] = res2['Cantidad']
        df['Referencia'] = res2['Referencia_Clean']
        
        def calc_precio(row):
            cant = row['Cantidad']
            if cant != 0.0:
                imp = row[col_importe] 
                return abs(imp / cant)
            return 0.0
            
        df['Precio'] = df.apply(calc_precio, axis=1)

    # Limpieza final de columnas
    col_liquida = [c for c in df.columns if 'Liquida' in c]
    if col_liquida:
        df[col_liquida[0]] = pd.to_datetime(df[col_liquida[0]], format='%d/%m/%y', errors='coerce').dt.strftime('%Y-%m-%d')
        
    rename_cols = {}
    for c in df.columns:
        if 'Cpbt' in c:
            rename_cols[c] = 'Comprobante'
    df.rename(columns=rename_cols, inplace=True)
    
    if col_ref_cant_precio:
        df.drop(columns=[col_ref_cant_precio[0]], inplace=True)
    if col_espcantref:
        df.drop(columns=[col_espcantref[0]], inplace=True)

    # Reordenamiento y normalización final del esquema
    # 1. Crear 'Operado' basada en 'Liquida' si existe
    if 'Liquida' in df.columns:
        df['Operado'] = df['Liquida']
        
    # 2. Reordenar el DataFrame para esquema 1 a 1
    orden_columnas = ['Liquida', 'Operado', 'Comprobante', 'Numero', 'Cantidad', 'Especie', 'Precio', 'Importe', 'Saldo', 'Referencia']
    
    for c in orden_columnas:
        if c not in df.columns:
            df[c] = None
            
    df = df[orden_columnas]

    # Guardar archivo con prefijo clean_
    ruta_out = os.path.join(cuentas_dir, f"clean_{archivo}")
    df.to_csv(ruta_out, index=False)
    print(f"Limpiado y guardado: {ruta_out} con {len(df)} filas.")

print("Transformacion completada exitosamente.")
