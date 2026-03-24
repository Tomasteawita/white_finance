"""Clase abstracta para validadores de datos. Debe tener un metodo abstracto para leer un archivo a partir de un path, otro metodo abstracto para validar
, un metodo para transformar los datos y un metodo escribir el resultado"""

from abc import ABC, abstractmethod

class Validator(ABC):
    """Clase abstracta para validadores de datos."""

    def __init__(self, file_path, output_path):
        self.file_path = file_path
        self.output_path = output_path

    @abstractmethod
    def read_file(self):
        pass

    @abstractmethod
    def validate(self, df_data) -> dict:
        pass
    @abstractmethod
    def transform(self, valid_dict, df_data):
        pass

    def write_result(self, df_data):
        try:
            df_data.to_csv(self.output_path, index=False)
        except Exception as e:
            raise ValueError(f"Error al escribir el resultado: {e}")
