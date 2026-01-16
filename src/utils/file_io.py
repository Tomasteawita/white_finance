"""
Utilidades para manejo de archivos
"""

import pandas as pd
from pathlib import Path
from typing import List, Optional
import yaml


def load_yaml_config(config_path: Path) -> dict:
    """Carga archivo YAML de configuración"""
    with open(config_path, 'r', encoding='utf-8') as f:
        return yaml.safe_load(f)


def save_yaml_config(config: dict, config_path: Path):
    """Guarda configuración en archivo YAML"""
    with open(config_path, 'w', encoding='utf-8') as f:
        yaml.dump(config, f, default_flow_style=False, allow_unicode=True)


def find_latest_file(directory: Path, pattern: str) -> Optional[Path]:
    """
    Encuentra el archivo más reciente que coincida con un patrón
    """
    files = list(directory.glob(pattern))
    if not files:
        return None
    
    # Ordenar por fecha de modificación
    return max(files, key=lambda p: p.stat().st_mtime)


def list_client_files(raw_data_path: Path, client_id: str) -> List[Path]:
    """
    Lista todos los archivos de un cliente en data/01_raw
    """
    client_folder = raw_data_path / client_id
    if not client_folder.exists():
        return []
    
    return list(client_folder.glob("*.xlsx")) + list(client_folder.glob("*.xls"))


def ensure_directories(base_path: Path):
    """
    Asegura que existan todos los directorios necesarios
    """
    directories = [
        base_path / "01_raw",
        base_path / "02_processed" / "prices",
        base_path / "02_processed" / "accounts",
        base_path / "03_analytics" / "reports"
    ]
    
    for directory in directories:
        directory.mkdir(parents=True, exist_ok=True)
    
    print(f"✓ Estructura de directorios verificada en {base_path}")


def read_excel_smart(file_path: Path, **kwargs) -> pd.DataFrame:
    """
    Lee archivo Excel con manejo inteligente de errores
    """
    try:
        return pd.read_excel(file_path, **kwargs)
    except Exception as e:
        print(f"Error leyendo {file_path}: {e}")
        # Intentar con motor diferente
        try:
            return pd.read_excel(file_path, engine='openpyxl', **kwargs)
        except Exception as e2:
            print(f"Error con motor alternativo: {e2}")
            raise


def export_dataframe(df: pd.DataFrame, output_path: Path, format: str = 'csv'):
    """
    Exporta DataFrame en diferentes formatos
    """
    output_path.parent.mkdir(parents=True, exist_ok=True)
    
    if format == 'csv':
        df.to_csv(output_path, index=False)
    elif format == 'excel':
        df.to_excel(output_path, index=False)
    elif format == 'parquet':
        df.to_parquet(output_path, index=False)
    else:
        raise ValueError(f"Formato no soportado: {format}")
    
    print(f"✓ Archivo guardado: {output_path}")
