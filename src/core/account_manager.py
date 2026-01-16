"""
Gestor de cuentas corrientes - Lógica de merge/upsert
"""

import pandas as pd
from pathlib import Path
from typing import Optional
from datetime import datetime


class AccountManager:
    """
    Gestiona el histórico de cuentas corrientes de clientes
    Implementa lógica de merge incremental (upsert)
    """
    
    def __init__(self, processed_data_path: Path):
        self.processed_data_path = processed_data_path
        self.accounts_path = processed_data_path / "accounts"
        self.accounts_path.mkdir(parents=True, exist_ok=True)
    
    def get_historical_account(self, client_id: str) -> Optional[pd.DataFrame]:
        """
        Carga el histórico de cuenta corriente de un cliente
        """
        file_path = self.accounts_path / f"{client_id}_cc_historica.csv"
        
        if file_path.exists():
            df = pd.read_csv(file_path, parse_dates=['fecha'])
            return df
        return None
    
    def merge_new_transactions(self, client_id: str, new_transactions: pd.DataFrame) -> pd.DataFrame:
        """
        Combina nuevas transacciones con el histórico existente
        Evita duplicados y mantiene orden cronológico
        """
        historical = self.get_historical_account(client_id)
        
        if historical is None or historical.empty:
            # Primera vez: guardar directamente
            merged = new_transactions.copy()
        else:
            # Combinar y eliminar duplicados
            merged = pd.concat([historical, new_transactions], ignore_index=True)
            
            # Eliminar duplicados basados en fecha, ticker y tipo_operacion
            merged = merged.drop_duplicates(
                subset=['fecha', 'ticker', 'tipo_operacion', 'cantidad', 'precio'],
                keep='last'
            )
        
        # Ordenar por fecha
        merged = merged.sort_values('fecha').reset_index(drop=True)
        
        return merged
    
    def save_historical_account(self, client_id: str, df: pd.DataFrame):
        """
        Guarda el histórico actualizado de cuenta corriente
        """
        file_path = self.accounts_path / f"{client_id}_cc_historica.csv"
        df.to_csv(file_path, index=False)
        print(f"✓ Histórico guardado: {file_path}")
    
    def update_account(self, client_id: str, new_transactions: pd.DataFrame):
        """
        Actualiza el histórico con nuevas transacciones (proceso completo)
        """
        merged = self.merge_new_transactions(client_id, new_transactions)
        self.save_historical_account(client_id, merged)
        
        print(f"Cliente {client_id}:")
        print(f"  - Transacciones nuevas: {len(new_transactions)}")
        print(f"  - Total histórico: {len(merged)}")
        
        return merged
    
    def get_account_summary(self, client_id: str) -> dict:
        """
        Genera resumen de cuenta corriente
        """
        df = self.get_historical_account(client_id)
        
        if df is None or df.empty:
            return {"error": "No hay datos históricos"}
        
        return {
            "cliente": client_id,
            "transacciones_totales": len(df),
            "fecha_inicio": df['fecha'].min(),
            "fecha_fin": df['fecha'].max(),
            "saldo_actual": df['saldo'].iloc[-1] if 'saldo' in df.columns else None
        }
