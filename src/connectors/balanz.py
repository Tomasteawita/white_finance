"""
Conector para Balanz Capital
"""

import pandas as pd
from pathlib import Path
from typing import Dict, List
from .base_strategy import BaseBrokerStrategy


class BalanzStrategy(BaseBrokerStrategy):
    """
    Implementación específica para Balanz Capital
    """
    
    def __init__(self):
        super().__init__("balanz")
    
    def read_cuenta_corriente(self, file_path: Path) -> pd.DataFrame:
        """
        Lee archivo de cuenta corriente de Balanz
        """
        # TODO: Implementar lógica específica de Balanz
        df = pd.read_excel(file_path)
        
        # Normalizar columnas según el formato de Balanz
        df_normalized = pd.DataFrame({
            'fecha': pd.to_datetime(df['Fecha de Liquidación']),
            'tipo_operacion': df['Tipo de Operación'],
            'ticker': df['Ticker'],
            'cantidad': df['Cantidad'],
            'precio': df['Precio Unitario'],
            'monto': df['Monto Total'],
            'saldo': df['Saldo en Cuenta']
        })
        
        return df_normalized
    
    def read_cartera(self, file_path: Path) -> pd.DataFrame:
        """
        Lee archivo de cartera de Balanz
        """
        # TODO: Implementar lógica específica de Balanz
        df = pd.read_excel(file_path, sheet_name='Tenencias')
        
        df_normalized = pd.DataFrame({
            'ticker': df['Ticker'],
            'cantidad': df['Tenencia'],
            'precio_promedio': df['Precio Promedio de Compra'],
            'ultimo_precio': df['Cotización'],
            'valor_actual': df['Valuación']
        })
        
        return df_normalized
    
    def validate_file(self, file_path: Path) -> bool:
        """
        Valida si el archivo es de Balanz
        """
        try:
            df = pd.read_excel(file_path, nrows=5)
            # Verificar columnas típicas de Balanz
            balanz_columns = ['Fecha de Liquidación', 'Tipo de Operación', 'Ticker']
            return all(col in df.columns for col in balanz_columns)
        except Exception:
            return False
