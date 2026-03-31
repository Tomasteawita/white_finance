import pandas as pd
import re
from pathlib import Path
from datetime import datetime
from typing import Optional

DATA_DIR = Path("../../../data/analytics/marketing_datosdemercado")


def get_date_from_filename(filename: str) -> Optional[datetime]:
    """Extrae la fecha embebida en el nombre de un archivo con formato YYYYMMDD.

    Busca el patron de 8 digitos seguidos (ej: instagram_reels-20260314.csv).
    Retorna None si no se encuentra ninguna fecha valida.
    """
    match = re.search(r"(\d{8})", filename)
    if not match:
        return None
    try:
        return datetime.strptime(match.group(1), "%Y%m%d")
    except ValueError:
        return None


def process_platform(
    data_dir: Path,
    historic_filename: str,
    file_prefix: str,
) -> tuple[bool, pd.DataFrame, list]:
    """Levanta el historico, detecta archivos nuevos y valida columnas.

    Pasos:
    1. Lee el CSV historico y detecta la ultima fecha reportada (report_date).
    2. Busca archivos cuyo nombre contenga una fecha posterior a la del historico.
    3. Valida que los archivos nuevos tengan las mismas columnas que el historico
       (excluyendo report_date, que se asigna en el paso de ETL).

    Returns:
        (all_valid, df_historic, new_files) donde all_valid es True solo si
        todos los archivos nuevos pasan la validacion de columnas.
    """
    print(f"\n{'='*60}")
    print(f"Procesando: {historic_filename}")
    print(f"{'='*60}")

    # --- 1. Leer historico completo ---
    historic_path = data_dir / historic_filename
    if not historic_path.exists():
        raise FileNotFoundError(f"Historico no encontrado: {historic_path}")

    df_historic = pd.read_csv(historic_path)
    print(f"  Historico cargado: {len(df_historic)} filas | Columnas: {list(df_historic.columns)}")

    # --- 2. Detectar la fecha maxima en report_date (formato YYYY-MM-DD) ---
    if "report_date" not in df_historic.columns:
        raise ValueError(f"La columna 'report_date' no existe en {historic_filename}")

    df_historic["report_date"] = pd.to_datetime(df_historic["report_date"], format="%Y-%m-%d")
    max_report_date: datetime = df_historic["report_date"].max()
    print(f"  Ultima fecha en historico (report_date): {max_report_date.date()}")

    # --- 3. Buscar archivos nuevos con fecha mayor en el nombre (formato YYYYMMDD) ---
    # report_date se agrega antes de concatenar con el historico, NO existe en los archivos nuevos
    historic_columns = set(df_historic.columns) - {"report_date"}
    candidate_files = [
        f for f in data_dir.iterdir()
        if f.is_file()
        and f.name.startswith(file_prefix)
        and f.name != historic_filename
        and f.suffix == ".csv"
    ]

    new_files: list[tuple[Path, datetime]] = []
    for f in candidate_files:
        file_date = get_date_from_filename(f.name)
        if file_date is not None and file_date > max_report_date:
            new_files.append((f, file_date))

    new_files.sort(key=lambda x: x[1])  # Ordenar cronologicamente

    if not new_files:
        print("  No se encontraron archivos nuevos con fecha posterior al historico.")
        return True, df_historic, []

    print(f"  Archivos nuevos encontrados: {[f.name for f, _ in new_files]}")

    # --- 4. Validar columnas de cada archivo nuevo contra el historico ---
    all_valid = True
    for new_file, file_date in new_files:
        df_new = pd.read_csv(new_file)
        new_columns = set(df_new.columns)

        missing_cols = historic_columns - new_columns
        extra_cols = new_columns - historic_columns

        print(f"\n  Archivo: {new_file.name} (fecha: {file_date.date()})")
        print(f"    Filas: {len(df_new)}")

        if not missing_cols and not extra_cols:
            print("    OK: Columnas coinciden exactamente con el historico.")
        else:
            all_valid = False
            if missing_cols:
                print(f"    ERROR - Columnas FALTANTES en el archivo nuevo: {sorted(missing_cols)}")
            if extra_cols:
                print(f"    WARN  - Columnas EXTRA en el archivo nuevo (no estan en historico): {sorted(extra_cols)}")

    if all_valid:
        print(f"\n  Todos los archivos nuevos son compatibles con el historico.")
    else:
        print(f"\n  ERROR: Se detectaron inconsistencias de columnas. Revisa los archivos antes de continuar.")

    return all_valid, df_historic, new_files


def run_etl(
    df_historic: pd.DataFrame,
    new_files: list[tuple[Path, datetime]],
    join_keys: list[str],
    historic_path: Path,
) -> None:
    """ETL incremental: full outer join entre el historico y cada archivo nuevo.

    Logica de prioridad:
    - Para registros presentes en ambos dataframes, se conservan los valores
      del archivo nuevo (datos mas recientes).
    - Para registros solo en el historico, se mantienen sin cambios.
    - Para registros solo en el archivo nuevo, se agregan.
    - report_date se asigna a partir de la fecha embebida en el nombre del archivo.

    Los archivos se procesan en orden cronologico; el historico se actualiza
    iterativamente si hay mas de un archivo nuevo.

    Args:
        df_historic: DataFrame del historico ya cargado (con report_date como datetime).
        new_files: Lista de (Path, datetime) de archivos nuevos, ordenados por fecha.
        join_keys: Columnas clave para el join (ej: ['Post ID', 'Account ID']).
        historic_path: Ruta del CSV historico donde se guardara el resultado.
    """
    print(f"\n{'='*60}")
    print(f"ETL: actualizando {historic_path.name}")
    print(f"{'='*60}")

    # Preservar el orden original de columnas del historico
    column_order = list(df_historic.columns)

    # Convertir report_date a string para serializar sin problemas
    df_current = df_historic.copy()
    df_current["report_date"] = df_current["report_date"].dt.strftime("%Y-%m-%d")

    for new_file, file_date in new_files:
        print(f"\n  Procesando: {new_file.name} (report_date = {file_date.date()})")

        df_new = pd.read_csv(new_file)

        # Asignar report_date al archivo nuevo a partir de la fecha del nombre
        df_new["report_date"] = file_date.strftime("%Y-%m-%d")

        # Full outer join entre el estado actual del historico y el archivo nuevo
        df_merged = df_current.merge(
            df_new,
            on=join_keys,
            how="outer",
            suffixes=("_hist", "_new"),
        )

        # Para cada columna no-clave, priorizar el valor del archivo nuevo
        non_key_cols = [c for c in column_order if c not in join_keys]
        for col in non_key_cols:
            col_new = f"{col}_new"
            col_hist = f"{col}_hist"
            if col_new in df_merged.columns:
                # combine_first: usa col_new y rellena nulos con col_hist
                df_merged[col] = df_merged[col_new].combine_first(df_merged[col_hist])
                df_merged.drop(columns=[col_hist, col_new], inplace=True)

        # Restaurar el orden de columnas del historico
        df_current = df_merged[column_order].copy()

        registros_nuevos = len(df_merged) - len(df_current.dropna(subset=join_keys, how="all"))
        print(f"    Filas historico antes: {len(df_current)} | "
              f"Filas archivo nuevo: {len(df_new)} | "
              f"Filas resultado: {len(df_current)}")

    # Guardar historico actualizado
    df_current.to_csv(historic_path, index=False)
    print(f"\n  Historico actualizado y guardado: {historic_path}")
    print(f"  Total registros: {len(df_current)}")


# ─── Ejecucion ────────────────────────────────────────────────────────────────

valid_ig, df_hist_ig, new_files_ig = process_platform(
    data_dir=DATA_DIR,
    historic_filename="instagram_reels_historic.csv",
    file_prefix="instagram_reels",
)

if valid_ig and new_files_ig:
    run_etl(
        df_historic=df_hist_ig,
        new_files=new_files_ig,
        join_keys=["Post ID", "Account ID"],
        historic_path=DATA_DIR / "instagram_reels_historic.csv",
    )
elif not valid_ig:
    print("\nETL Instagram omitido: fallo la validacion de columnas.")

valid_yt, df_hist_yt, new_files_yt = process_platform(
    data_dir=DATA_DIR,
    historic_filename="youtube_shorts_historic.csv",
    file_prefix="youtube_shorts",
)

if valid_yt and new_files_yt:
    run_etl(
        df_historic=df_hist_yt,
        new_files=new_files_yt,
        join_keys=["Content"],
        historic_path=DATA_DIR / "youtube_shorts_historic.csv",
    )
elif not valid_yt:
    print("\nETL YouTube omitido: fallo la validacion de columnas.")
