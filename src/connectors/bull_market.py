"""
Conector para Bull Market Brokers
"""

import pandas as pd
from pathlib import Path
from typing import Dict, List
from .base_strategy import BaseBrokerStrategy


class BullMarketStrategy(BaseBrokerStrategy):
    """
    Implementación específica para Bull Market Brokers
    """
    
    def __init__(self):
        super().__init__("bull_market")
    
    def read_cuenta_corriente(self, file_path: Path) -> pd.DataFrame:
        """
        Lee archivo de cuenta corriente de Bull Market
        """
        # TODO: Implementar lógica específica de Bull Market
        df = pd.read_excel(file_path)
        
        # Normalizar columnas según el formato de Bull Market
        df_normalized = pd.DataFrame({
            'fecha': pd.to_datetime(df['Fecha']),
            'tipo_operacion': df['Operación'],
            'ticker': df['Especie'],
            'cantidad': df['Cantidad'],
            'precio': df['Precio'],
            'monto': df['Monto'],
            'saldo': df['Saldo']
        })
        
        return df_normalized
    
    def read_cartera(self, file_path: Path) -> pd.DataFrame:
        """
        Lee archivo de cartera de Bull Market
        """
        # TODO: Implementar lógica específica de Bull Market
        df = pd.read_excel(file_path, sheet_name='Cartera')
        
        df_normalized = pd.DataFrame({
            'ticker': df['Especie'],
            'cantidad': df['Cantidad'],
            'precio_promedio': df['Precio Promedio'],
            'ultimo_precio': df['Último Precio'],
            'valor_actual': df['Valor Actual']
        })
        
        return df_normalized
    
    def validate_file(self, file_path: Path) -> bool:
        """
        Valida si el archivo es de Bull Market
        """
        try:
            df = pd.read_excel(file_path, nrows=5)
            # Verificar columnas típicas de Bull Market
            bull_columns = ['Fecha', 'Operación', 'Especie']
            return all(col in df.columns for col in bull_columns)
        except Exception:
            return False
