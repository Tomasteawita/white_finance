import pandas as pd
import os

base_dir = "/home/jovyan/data"

archivos = [
    "historico_completo_dolares_cable.csv",
    "historico_completo_dolares_mep.csv",
    "historico_completo_pesos.csv"
]

for archivo in archivos:
    ruta = os.path.join(base_dir, archivo)
    if not os.path.exists(ruta):
        continue
    
    df = pd.read_csv(ruta)
    
    # Filtrar solo Compras y Ventas
    df_cv = df[df['Comprobante'].fillna('').str.contains('COMPRA|VENTA', case=False, regex=True)].copy()
    
    if df_cv.empty:
        print(f"[{archivo}] No hay transacciones de compra/venta para validar.\n")
        continue
        
    print(f"[{archivo}] Validando {len(df_cv)} operaciones de COMPRA/VENTA...")
    
    errores = []
    
    for idx, row in df_cv.iterrows():
        comp = str(row['Comprobante']).upper()
        cant = float(row.get('Cantidad', 0)) if pd.notna(row.get('Cantidad')) else 0.0
        imp = float(row.get('Importe', 0)) if pd.notna(row.get('Importe')) else 0.0
        precio = float(row.get('Precio', 0)) if pd.notna(row.get('Precio')) else 0.0
        num = row.get('Numero', 'N/A')
        
        # 1. Regla del Precio: siempre positivo o cero
        if precio < 0:
            errores.append(f"Op {num} ({comp}): Precio negativo ({precio})")
            
        # 2. Reglas de Compra
        if 'COMPRA' in comp:
            if cant < 0:
                errores.append(f"Op {num} ({comp}): Cantidad negativa ({cant}) siendo una compra")
            if imp > 0:
                errores.append(f"Op {num} ({comp}): Importe positivo ({imp}) siendo una compra")
                
        # 3. Reglas de Venta
        elif 'VENTA' in comp:
            if cant > 0:
                errores.append(f"Op {num} ({comp}): Cantidad positiva ({cant}) siendo una venta")
            if imp < 0:
                errores.append(f"Op {num} ({comp}): Importe negativo ({imp}) siendo una venta")
                
        # 4. Signos opuestos entre Cantidad e Importe (si ambos son != 0)
        if cant != 0 and imp != 0:
            if (cant > 0 and imp > 0) or (cant < 0 and imp < 0):
                errores.append(f"Op {num} ({comp}): Rompe asimetría de signos -> Cantidad ({cant}) e Importe ({imp})")
                
    if errores:
        print(f"  -> ATENCION: Se encontraron {len(errores)} anomalías:")
        for e in errores[:15]:
            print(f"     * {e}")
        if len(errores) > 15:
            print(f"     ... y {len(errores) - 15} errores mas.")
    else:
        print("  -> EXCELENTE: Todas las operaciones siguen el patrón de signos bursátil correctamente.")
    
    print("-" * 60)
