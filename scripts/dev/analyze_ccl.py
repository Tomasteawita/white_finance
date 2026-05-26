import pandas as pd
import numpy as np
from typing import List, Dict, Any
import logging

# Configuración de Logging para trazabilidad CNV
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

def analyze_cash_ccl_movements(file_path: str) -> List[Dict[str, Any]]:
    """
    Analiza y extrae los movimientos históricos y recientes de Cash_CCL a partir de holdings diarios.
    
    Lógica de Negocio:
    - Identifica cambios de tenencia en la cuenta corriente de Cash_CCL (variaciones de liquidez CCL).
    - Permite trazar ingresos y egresos de moneda extranjera (CCL) para auditoría e ingeniería de datos.

    Args:
        file_path: Ruta al archivo CSV de holdings diarios.

    Returns:
        Lista de diccionarios con el detalle de cada variación encontrada.
    """
    try:
        # Validación de datos: Cargar CSV de forma segura
        df = pd.read_csv(file_path)
        
        # Renombrar la primera columna a 'Date' si está vacía
        if df.columns[0] == '' or df.columns[0].startswith('Unnamed:'):
            df.rename(columns={df.columns[0]: 'Date'}, inplace=True)
        
        if 'Date' not in df.columns or 'Cash_CCL' not in df.columns:
            raise ValueError("El archivo no contiene las columnas necesarias ('Date' y 'Cash_CCL').")
        
        # Parseo de fechas y selección de columnas relevantes
        df['Date'] = pd.to_datetime(df['Date'])
        df = df[['Date', 'Cash_CCL']].copy()
        
        # Validar nulos y rellenar si es necesario (en holdings asumimos forward fill o 0)
        df['Cash_CCL'] = df['Cash_CCL'].fillna(0.0)
        
        # Identificar cambios
        df['Prev_Cash_CCL'] = df['Cash_CCL'].shift(1)
        
        # El primer día no tiene previo, asignamos 0 o el mismo valor para evitar registrar cambio falso
        df.iloc[0, df.columns.get_loc('Prev_Cash_CCL')] = df.iloc[0, df.columns.get_loc('Cash_CCL')]
        
        # Filtrar donde haya diferencias
        df['Change'] = df['Cash_CCL'] - df['Prev_Cash_CCL']
        changes_df = df[df['Change'] != 0.0].copy()
        
        movements = []
        for _, row in changes_df.iterrows():
            movements.append({
                'Date': row['Date'].strftime('%Y-%m-%d'),
                'Previous_Value': float(row['Prev_Cash_CCL']),
                'New_Value': float(row['Cash_CCL']),
                'Change': float(row['Change'])
            })
            
        return movements

    except Exception as e:
        logging.error(f"Error al analizar holdings: {e}", exc_info=True)
        raise

if __name__ == "__main__":
    csv_path = "c:/Users/tomas/white_finance/data/analytics/portfolio_visualization_data/holdings_diarios.csv"
    logging.info("Iniciando análisis de movimientos de Cash_CCL...")
    
    try:
        movs = analyze_cash_ccl_movements(csv_path)
        logging.info(f"Análisis completado. Se encontraron {len(movs)} variaciones en total.")
        
        # Mostrar los últimos 15 movimientos
        print("\n=== ÚLTIMOS 15 MOVIMIENTOS DE CASH_CCL ===")
        print(f"{'Fecha':<12} | {'Valor Anterior':<15} | {'Valor Nuevo':<15} | {'Variación':<15}")
        print("-" * 65)
        for m in movs[-15:]:
            sign = "+" if m['Change'] > 0 else ""
            print(f"{m['Date']:<12} | {m['Previous_Value']:15.2f} | {m['New_Value']:15.2f} | {sign}{m['Change']:15.2f}")
            
    except Exception:
        logging.critical("Fallo catastrófico en la ejecución del script.")
