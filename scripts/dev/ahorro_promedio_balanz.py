import os
import pandas as pd
import numpy as np
from pathlib import Path
from typing import List, Dict, Any

def calcular_ahorro_promedio() -> None:
    """
    Calcula el ahorro promedio mensual por cliente en Balanz.
    
    Lógica de negocio:
    El ahorro recurrente de los clientes en sus cuentas comitentes es una medida de liquidez excedente 
    disponible para inversión de corto/mediano plazo. Se define como aquellos registros donde 
    la 'Descripción' de la transacción indique 'Recibo de Cobro', que denota transferencias 
    entrantes en ARS al broker.
    
    El cálculo agrupa por periodo (YYYY-MM) para normalizar ingresos esporádicos a lo largo del mes,
    y luego promedia sobre los meses en que hubo efectivamente inyección de capital.
    """
    data_dir: Path = Path(r"c:\Users\tomas\white_finance\data\balanz")
    
    if not data_dir.exists():
        print(f"El directorio {data_dir} no existe.")
        return

    resultados: List[Dict[str, Any]] = []

    for cliente_dir in data_dir.iterdir():
        if not cliente_dir.is_dir():
            continue
            
        cliente_nombre = cliente_dir.name
        csv_path = cliente_dir / "Cuenta Corriente" / "cuenta_corriente_historico.csv"
        
        if not csv_path.exists():
            print(f"No se encontró archivo histórico para {cliente_nombre}")
            continue
            
        try:
            # Leer el csv, separador es pipe
            df = pd.read_csv(csv_path, sep='|')
            
            # Limpiar nombres de columnas por si tienen espacios
            df.columns = df.columns.str.strip()
            
            # Asegurarse de que las columnas requeridas existen
            if 'Fecha' not in df.columns or 'Descripción' not in df.columns or 'Importe' not in df.columns:
                print(f"Faltan columnas requeridas para {cliente_nombre}")
                continue
                
            # Parsear fecha
            df['Fecha'] = pd.to_datetime(df['Fecha'], errors='coerce')
            
            # Filtrar por Recibo de Cobro en la descripción
            df_ahorro = df[df['Descripción'].str.contains("Recibo de Cobro", na=False, case=False)].copy()
            
            if df_ahorro.empty:
                resultados.append({
                    "Cliente": cliente_nombre,
                    "Ahorro Promedio Mensual": 0.0,
                    "Meses c/ Ahorro": 0
                })
                continue
                
            # Asegurar importe numérico
            df_ahorro['Importe'] = pd.to_numeric(df_ahorro['Importe'], errors='coerce').fillna(0)
            
            # Extraer periodo (Año-Mes)
            df_ahorro['Periodo'] = df_ahorro['Fecha'].dt.to_period('M')
            
            # Sumarizar importe por periodo
            ahorro_por_mes = df_ahorro.groupby('Periodo')['Importe'].sum()
            
            # Calcular promedio sobre los meses que hubo ahorro
            promedio = ahorro_por_mes.mean()
            meses_distintos = len(ahorro_por_mes)
            
            resultados.append({
                "Cliente": cliente_nombre,
                "Ahorro Promedio Mensual": promedio,
                "Meses c/ Ahorro": meses_distintos
            })
            
        except Exception as e:
            print(f"Error procesando cliente {cliente_nombre}: {e}")
            
    # Mostrar resultados
    if resultados:
        df_resultados = pd.DataFrame(resultados)
        print("\n--- AHORRO PROMEDIO MENSUAL POR CLIENTE ---")
        
        # Formatear el promedio para mejor lectura
        df_resultados['Ahorro Promedio Mensual'] = df_resultados['Ahorro Promedio Mensual'].apply(lambda x: f"${x:,.2f}")
        
        # Ajustar opciones de pandas para imprimir todo el df limpio
        pd.set_option('display.max_columns', None)
        pd.set_option('display.width', 1000)
        print(df_resultados.to_string(index=False))
    else:
        print("No se encontraron resultados.")

if __name__ == "__main__":
    calcular_ahorro_promedio()
