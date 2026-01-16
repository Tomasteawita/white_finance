"""
Cliente de las clases validadores de datos.
"""
# importo la clase que importa modulos
from importlib import import_module
import argparse

def main(**kwargs):
    """
    Función principal que recibe el nombre del validador y los argumentos necesarios.
    """
    file_path = kwargs.get('file_path')
    output_path = kwargs.get('output_path')
    validator_name = kwargs.get('validator_name')
    if not file_path or not output_path or not validator_name:
        raise ValueError("Los argumentos 'file_path', 'output_path' y 'validator_name' son obligatorios.")
    module = import_module(validator_name)
    validator_class_name = validator_name.replace('_', ' ')
    validator_class_name = validator_class_name.title().replace(' ', '')
    # le agrego la palabra Validator al final del nombre de la clase
    validator_class_name += 'Validator'
    print(f"Importando la clase {validator_class_name} del módulo {module.__name__}")
    validator_class = getattr(module, validator_class_name)
    validator = validator_class(file_path, output_path)
    df_data = validator.read_file()
    dict_valid = validator.validate(df_data)
    df_transformed = validator.transform(dict_valid,df_data)
    validator.write_result(df_transformed)

    print("Validación y transformación completadas. Resultado guardado en:", output_path)
if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Cliente de validación de datos")
    parser.add_argument("--file_path", required=True, help="Ruta del archivo de entrada")
    parser.add_argument("--output_path", required=True, help="Ruta del archivo de salida")
    parser.add_argument("--validator_name", required=True, help="Nombre del validador a utilizar")
    args = parser.parse_args()
    main(**vars(args))
