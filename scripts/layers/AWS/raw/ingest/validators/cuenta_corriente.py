"""
Script para validar y procesar el archivo excel de cuenta corriente de BullMarket para antes de su ingesta en S3.
Importa la clase Validator del validator.py
"""
from validator import Validator
import pandas as pd

class CuentaCorrienteValidator(Validator):
    def read_file(self):
        """Lee el archivo Excel y lo convierte a un DataFrame de pandas."""
        df = pd.read_excel(self.file_path, sheet_name=None)
        # obtengo el nombre de la hoja
        sheet_name = list(df.keys())[0]
        # obtengo el dataframe de la hoja
        df = df[sheet_name]
        # impritmo el tipo de dato de cada columna
        print("Tipos de datos de las columnas:")
        print(df.dtypes)
        # muestro las primeras filas del dataframe
        df.head()
        return df
    
    def _datatime_format(self, df_cuenta_corriente):
        """Si las columnas Liquida u Operado son de tipo fecha con formato YYYY-MM-DD
        devuelvo True, caso contrario False."""
        try:
            pd.to_datetime(df_cuenta_corriente['Liquida'], format='%Y-%m-%d')
            pd.to_datetime(df_cuenta_corriente['Operado'], format='%Y-%m-%d')
            return True
        except ValueError:
            return False
    
    def _cols_not_trimmed(self, df_cuenta_corriente):
        """
        Verifica si las columnas Comprobante, Especie y Referencia tienen espacios al inicio o al final.
        """
        cols_to_check = ['Comprobante', 'Especie', 'Referencia']
        for col in cols_to_check:
            if df_cuenta_corriente[col].str.strip().ne(df_cuenta_corriente[col]).any():
                return True
        return False
    def _cols_are_int(self, df_cuenta_corriente):
        """
        Verifica si la columna Numero es de tipo entero.
        """
        return pd.api.types.is_integer_dtype(df_cuenta_corriente['Numero'])
    
    def _cols_are_float(self, df_cuenta_corriente):
        """
        Verifica si las columnas Cantidad, Precio, Importe y Saldo son de tipo float.
        """
        cols_to_check = ['Cantidad', 'Precio', 'Importe', 'Saldo']
        for col in cols_to_check:
            if not pd.api.types.is_float_dtype(df_cuenta_corriente[col]):
                return False
        return True

    def validate(self, df_data) -> dict:
        """Valida que los datos contengan las columnas esperadas."""
        required_columns = ['Liquida', 'Operado', 'Comprobante', 'Especie', 'Referencia', 'Numero', 'Cantidad', 'Precio', 'Importe', 'Saldo']
        # Verifica si todas las columnas requeridas están presentes
        if not all(col in df_data.columns for col in required_columns):
            raise ValueError(f"Faltan columnas requeridas en el DataFrame: {set(required_columns) - set(df_data.columns)}")
        
        # uso los metodos de validación
        valid_dict = {
            'Formato de fecha': self._datatime_format(df_data),
            'Columnas con espacios': not self._cols_not_trimmed(df_data),
            'Columna Numero es int': self._cols_are_int(df_data),
            'Columnas Cantidad, Precio, Importe y Saldo son float': self._cols_are_float(df_data)
        }
        return valid_dict

    def transform(self, valid_dict, df_data):
        """Transforma los datos, por ejemplo, asegurando que las fechas estén en el formato correcto."""
        if not valid_dict['Formato de fecha']:
            df_data['Liquida'] = pd.to_datetime(df_data['Liquida'], format='%Y-%m-%d', errors='coerce')
            df_data['Operado'] = pd.to_datetime(df_data['Operado'], format='%Y-%m-%d', errors='coerce')
        if valid_dict['Columnas con espacios']:
            df_data['Comprobante'] = df_data['Comprobante'].str.strip()
            df_data['Especie'] = df_data['Especie'].str.strip()
            df_data['Referencia'] = df_data['Referencia'].str.strip()
        if not valid_dict['Columna Numero es int']:
            df_data['Numero'] = df_data['Numero'].astype(int)
        if not valid_dict['Columnas Cantidad, Precio, Importe y Saldo son float']:
            df_data['Cantidad'] = df_data['Cantidad'].astype(float)
            df_data['Precio'] = df_data['Precio'].astype(float)
            df_data['Importe'] = df_data['Importe'].astype(float)
            df_data['Saldo'] = df_data['Saldo'].astype(float)
        return df_data