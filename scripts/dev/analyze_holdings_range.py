import pandas as pd
import numpy as np
import sys
import os
from pathlib import Path
from typing import Dict, Any

# Añadir el directorio raíz al path de Python
root_path = Path(__file__).resolve().parent.parent.parent
if str(root_path) not in sys.path:
    sys.path.append(str(root_path))

from scripts.pipelines.portfolio_visualization.execute_evolucion_patrimonio import EvolucionHistoricaPatrimonio

def analyze_holdings_range(start_date: str, end_date: str) -> None:
    """
    Procesa las transacciones unificadas para calcular las tenencias diarias actualizadas
    y muestra cualquier movimiento (cambio en la composición de cartera) en el rango solicitado.
    
    Lógica de Negocio:
    - Reconstruye la serie temporal de holdings mediante forward-fill (ffill).
    - Compara día a día para identificar transacciones o variaciones de tenencia en el período.
    """
    # Inicializar la clase de la tubería de patrimonio
    pipeline = EvolucionHistoricaPatrimonio()
    
    # Cargar transacciones unificadas
    df = pd.read_csv(
        pipeline.path_cuentas_unificadas,
        parse_dates=['Operado', 'Liquida']
    )
    df = df.sort_values(by=['Operado', 'Numero']).reset_index(drop=True)
    
    # Procesar transacciones para obtener holdings diarios (con operaciones)
    holdings_diarios = pipeline.process_transactions(df)
    holdings_diarios.set_index('Operado', inplace=True)
    
    # Completar la serie temporal hasta hoy usando forward fill
    today = pd.Timestamp.today().normalize()
    idx = pd.date_range(holdings_diarios.index.min(), today)
    holdings = holdings_diarios.reindex(idx, method='ffill').fillna(0.0)
    
    # Filtrar por el rango solicitado
    start_dt = pd.to_datetime(start_date)
    end_dt = pd.to_datetime(end_date)
    
    # Tomar también el día anterior a start_date para poder ver el cambio en el primer día del rango
    prev_start_dt = start_dt - pd.Timedelta(days=1)
    
    holdings_range = holdings.loc[prev_start_dt:end_dt].copy()
    
    print(f"=== ANÁLISIS DE TENENCIAS ENTRE {start_date} Y {end_date} ===")
    
    # Encontrar las columnas (activos) que tienen tenencias no nulas en este rango
    active_columns = [col for col in holdings_range.columns if (holdings_range[col] != 0).any()]
    
    # Filtrar las columnas activas
    holdings_active = holdings_range[active_columns]
    
    # Calcular las diferencias día a día
    diffs = holdings_active.diff().dropna()
    
    # Mostrar cambios día a día
    changes_found = False
    for date, row in diffs.iterrows():
        # Identificar qué activos cambiaron
        changes = row[row != 0.0]
        if not changes.empty:
            changes_found = True
            print(f"\nFecha: {date.strftime('%Y-%m-%d')}")
            for ticker, change in changes.items():
                prev_val = holdings_active.loc[date - pd.Timedelta(days=1), ticker]
                new_val = holdings_active.loc[date, ticker]
                sign = "+" if change > 0 else ""
                print(f"  * {ticker:<10}: {prev_val:10.2f} -> {new_val:10.2f} ({sign}{change:.2f})")
                
    if not changes_found:
        print("\nNo se registraron movimientos (cambios en las cantidades de tenencias) en el rango de fechas solicitado.")
        print("Las tenencias permanecieron constantes:")
        # Mostrar tenencias constantes del último día
        last_date = holdings_active.index[-1]
        print(f"\nTenencias al {last_date.strftime('%Y-%m-%d')}:")
        for ticker, val in holdings_active.loc[last_date].items():
            if val != 0.0:
                print(f"  - {ticker:<10}: {val:10.2f}")

if __name__ == "__main__":
    # Rango solicitado por el usuario: del 2026-05-15 al 2026-05-21
    analyze_holdings_range("2026-05-15", "2026-05-21")
