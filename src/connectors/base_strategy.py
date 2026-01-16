"""
Estrategia base para conectores de brokers (Abstract Base Class)
"""

from abc import ABC, abstractmethod
from typing import Dict, List
import pandas as pd
from pathlib import Path


class BaseBrokerStrategy(ABC):
    """
    Clase abstracta que define la interfaz común para todos los brokers
    """
    
    def __init__(self, broker_name: str):
        self.broker_name = broker_name
    
    @abstractmethod
    def read_cuenta_corriente(self, file_path: Path) -> pd.DataFrame:
        """
        Lee un archivo de cuenta corriente del broker y lo normaliza
        
        Returns:
            DataFrame con columnas estandarizadas:
            - fecha: datetime
            - tipo_operacion: str (compra, venta, dividendo, etc.)
            - ticker: str
            - cantidad: float
            - precio: float
            - monto: float
            - saldo: float
        """
        pass
    
    @abstractmethod
    def read_cartera(self, file_path: Path) -> pd.DataFrame:
        """
        Lee un archivo de posiciones de cartera del broker
        
        Returns:
            DataFrame con columnas estandarizadas:
            - ticker: str
            - cantidad: float
            - precio_promedio: float
            - ultimo_precio: float
            - valor_actual: float
        """
        pass
    
    @abstractmethod
    def validate_file(self, file_path: Path) -> bool:
        """
        Valida si el archivo pertenece a este broker
        """
        pass
    
    def get_broker_name(self) -> str:
        """Retorna el nombre del broker"""
        return self.broker_name
