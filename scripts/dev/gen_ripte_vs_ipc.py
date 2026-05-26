"""
gen_ripte_vs_ipc.py
───────────────────
Genera un CSV con la evolución del RIPTE real vs. un RIPTE simulado que
sigue al IPC Nacional General mes a mes, junto con las variaciones
porcentuales mensuales de ambos.

Fuentes:
  - RIPTE: PDF oficial (Julio 1994 – Marzo 2026)
  - IPC Nacional General: Excel INDEC (Diciembre 2016 – Abril 2026)

Lógica:
  1. Extrae los datos RIPTE del PDF (período + monto).
  2. Extrae el índice IPC Nivel General del Excel.
  3. Determina el período común más antiguo (Dic 2016).
  4. Calcula el RIPTE simulado como si los sueldos hubieran seguido al IPC
     mensualmente, partiendo del RIPTE real de Dic 2016.
  5. Calcula variaciones porcentuales mensuales para ambas series.

Salida: CSV en data/argentina_economy/ripte_vs_ipc_analysis.csv
"""

import re
import logging
from pathlib import Path
from typing import Optional

import pandas as pd
import numpy as np
import pdfplumber

# ──────────────────────────────────────────────────────────────
# Logging (trazabilidad CNV)
# ──────────────────────────────────────────────────────────────
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
logger = logging.getLogger(__name__)

# ──────────────────────────────────────────────────────────────
# Paths
# ──────────────────────────────────────────────────────────────
BASE_DIR = Path(__file__).resolve().parent.parent.parent
DATA_DIR = BASE_DIR / "data" / "argentina_economy"
RIPTE_PDF = DATA_DIR / "RIPTE mensual 1994-marzo 2026.pdf"
IPC_XLS = DATA_DIR / "IPC - historical 9 years.xls"
OUTPUT_CSV = DATA_DIR / "ripte_vs_ipc_analysis.csv"

# Mapeo de meses en español a número
MESES_MAP: dict[str, int] = {
    "enero": 1, "febrero": 2, "marzo": 3, "abril": 4,
    "mayo": 5, "junio": 6, "julio": 7, "agosto": 8,
    "septiembre": 9, "octubre": 10, "noviembre": 11, "diciembre": 12,
}


def _parse_monto(raw: str) -> Optional[float]:
    """Parsea un monto en formato argentino ('$ 1.234.567,89') a float.

    Maneja separadores de miles con punto y decimal con coma,
    conforme a la convención del mercado argentino.

    Returns:
        float o None si el parseo falla.
    """
    try:
        cleaned = raw.replace("$", "").strip()
        cleaned = cleaned.replace(".", "")  # quitar separador de miles
        cleaned = cleaned.replace(",", ".")  # decimal
        return float(cleaned)
    except (ValueError, AttributeError):
        logger.warning(f"No se pudo parsear monto: '{raw}'")
        return None


def extract_ripte_from_pdf(pdf_path: Path) -> pd.DataFrame:
    """Extrae la serie RIPTE mensual desde el PDF oficial.

    Parsea cada página buscando el patrón:
      AñoMes $ monto variación% indice indice_nd

    Returns:
        DataFrame con columnas ['periodo', 'monto_ripte']
        donde 'periodo' es datetime (primer día del mes).

    Raises:
        FileNotFoundError: Si el PDF no existe.
        ValueError: Si no se extraen datos válidos.
    """
    if not pdf_path.exists():
        raise FileNotFoundError(f"No se encontró el PDF RIPTE en: {pdf_path}")

    logger.info(f"Extrayendo RIPTE desde: {pdf_path.name}")

    records: list[dict] = []
    current_year: Optional[int] = None

    # Regex para líneas de datos RIPTE
    # Ej: "Julio $ 874,87 - 100,00 100,00"
    # Ej: "Enero $ 1.646.344,54 0,8% 188.181,62 188.181,62"
    line_pattern = re.compile(
        r"^(Enero|Febrero|Marzo|Abril|Mayo|Junio|Julio|Agosto|Septiembre|Octubre|Noviembre|Diciembre)"
        r"\s+(\$\s*[\d.,]+)",
        re.IGNORECASE,
    )
    year_pattern = re.compile(r"^(\d{4})$")

    with pdfplumber.open(str(pdf_path)) as pdf:
        for page_num, page in enumerate(pdf.pages):
            text = page.extract_text()
            if not text:
                continue

            for line in text.split("\n"):
                line = line.strip()

                # Detectar año
                year_match = year_pattern.match(line)
                if year_match:
                    candidate = int(year_match.group(1))
                    if 1990 <= candidate <= 2030:
                        current_year = candidate
                    continue

                # Detectar línea de datos
                data_match = line_pattern.match(line)
                if data_match and current_year is not None:
                    mes_str = data_match.group(1).lower()
                    monto_str = data_match.group(2)
                    mes_num = MESES_MAP.get(mes_str)
                    monto = _parse_monto(monto_str)

                    if mes_num is not None and monto is not None:
                        periodo = pd.Timestamp(year=current_year, month=mes_num, day=1)
                        records.append({"periodo": periodo, "monto_ripte": monto})

    if not records:
        raise ValueError("No se extrajeron registros del PDF RIPTE.")

    df = pd.DataFrame(records)
    df = df.sort_values("periodo").reset_index(drop=True)

    # Validación defensiva: detectar duplicados
    duplicados = df[df.duplicated(subset=["periodo"], keep=False)]
    if not duplicados.empty:
        logger.warning(f"Se encontraron {len(duplicados)} períodos duplicados en RIPTE. Se toma el último.")
        df = df.drop_duplicates(subset=["periodo"], keep="last").reset_index(drop=True)

    logger.info(
        f"RIPTE extraído: {len(df)} registros | "
        f"Rango: {df['periodo'].min():%Y-%m} a {df['periodo'].max():%Y-%m}"
    )
    return df


def extract_ipc_from_excel(xls_path: Path) -> pd.DataFrame:
    """Extrae el índice IPC Nacional Nivel General desde el Excel del INDEC.

    Lee la hoja 'Índices IPC Cobertura Nacional', donde:
      - Fila 5 (0-indexed) contiene las fechas como columnas.
      - Fila 9 (0-indexed) contiene los valores del Nivel General.

    Returns:
        DataFrame con columnas ['periodo', 'ipc_nivel_general']

    Raises:
        FileNotFoundError: Si el archivo no existe.
        ValueError: Si no se pueden extraer datos válidos.
    """
    if not xls_path.exists():
        raise FileNotFoundError(f"No se encontró el Excel IPC en: {xls_path}")

    logger.info(f"Extrayendo IPC desde: {xls_path.name}")

    df_raw = pd.read_excel(str(xls_path), sheet_name=2, header=None)

    # Fila 5: fechas (columnas 1 en adelante)
    fechas_row = df_raw.iloc[5, 1:]
    # Fila 9: Nivel General
    valores_row = df_raw.iloc[9, 1:]

    records: list[dict] = []
    for fecha, valor in zip(fechas_row, valores_row):
        if pd.isna(fecha) or pd.isna(valor):
            continue
        try:
            periodo = pd.Timestamp(fecha)
            ipc_val = float(valor)
            records.append({"periodo": periodo, "ipc_nivel_general": ipc_val})
        except (ValueError, TypeError) as e:
            logger.warning(f"Error parseando IPC: fecha={fecha}, valor={valor} -> {e}")

    if not records:
        raise ValueError("No se extrajeron registros del Excel IPC.")

    df = pd.DataFrame(records)
    df["periodo"] = df["periodo"].dt.to_period("M").dt.to_timestamp()
    df = df.sort_values("periodo").reset_index(drop=True)

    logger.info(
        f"IPC extraído: {len(df)} registros | "
        f"Rango: {df['periodo'].min():%Y-%m} a {df['periodo'].max():%Y-%m}"
    )
    return df


def build_analysis_csv(
    df_ripte: pd.DataFrame,
    df_ipc: pd.DataFrame,
    output_path: Path,
) -> pd.DataFrame:
    """Construye el CSV de análisis RIPTE vs IPC.

    Lógica de negocio:
    ─────────────────
    1. Se toma el período común más antiguo (Dic 2016) como base.
    2. El RIPTE simulado parte del RIPTE real del período base y crece
       mensualmente al ritmo del IPC Nacional General.
    3. Se calculan variaciones % mensuales para ambas series.

    Args:
        df_ripte: DataFrame con ['periodo', 'monto_ripte']
        df_ipc: DataFrame con ['periodo', 'ipc_nivel_general']
        output_path: Ruta destino del CSV.

    Returns:
        DataFrame final con las 5 columnas solicitadas.
    """
    # Merge por período
    df = pd.merge(df_ripte, df_ipc, on="periodo", how="inner")
    df = df.sort_values("periodo").reset_index(drop=True)

    if df.empty:
        raise ValueError("No hay períodos comunes entre RIPTE e IPC.")

    logger.info(
        f"Períodos comunes: {len(df)} | "
        f"Rango: {df['periodo'].min():%Y-%m} a {df['periodo'].max():%Y-%m}"
    )

    # ── RIPTE simulado siguiendo al IPC ──
    # Base: el RIPTE real del primer mes común
    ripte_base = df["monto_ripte"].iloc[0]
    ipc_base = df["ipc_nivel_general"].iloc[0]

    logger.info(
        f"Período base: {df['periodo'].iloc[0]:%Y-%m} | "
        f"RIPTE base: ${ripte_base:,.2f} | IPC base: {ipc_base:.4f}"
    )

    # El sueldo simulado crece proporcionalmente al IPC
    # RIPTE_simulado(t) = RIPTE_base * (IPC(t) / IPC_base)
    df["monto_ripte_si_ipc"] = ripte_base * (df["ipc_nivel_general"] / ipc_base)

    # ── Variaciones porcentuales mensuales ──
    # Variación % RIPTE real respecto al mes anterior
    df["var_pct_ripte"] = df["monto_ripte"].pct_change() * 100

    # Variación % IPC respecto al mes anterior
    df["var_pct_ipc"] = df["ipc_nivel_general"].pct_change() * 100

    # ── Brecha porcentual RIPTE real vs simulado IPC ──
    # Positivo = sueldos superaron inflación, Negativo = sueldos perdieron contra inflación
    df["brecha_pct"] = ((df["monto_ripte"] / df["monto_ripte_si_ipc"]) - 1) * 100

    # ── Formatear salida ──
    df_output = pd.DataFrame({
        "Período": df["periodo"].dt.strftime("%Y-%m"),
        "Monto en $ RIPTE": df["monto_ripte"].round(2),
        "Monto en $ si los sueldos seguían al IPC": df["monto_ripte_si_ipc"].round(2),
        "Variación % respecto mes anterior RIPTE": df["var_pct_ripte"].round(2),
        "Variación % IPC Nacional General": df["var_pct_ipc"].round(2),
        "Brecha %": df["brecha_pct"].round(2),
    })

    # Guardar CSV
    df_output.to_csv(str(output_path), index=False, encoding="utf-8-sig")
    logger.info(f"CSV generado exitosamente: {output_path}")
    logger.info(f"Total de filas: {len(df_output)}")

    # Resumen rápido
    ultimo = df_output.iloc[-1]
    logger.info(
        f"Último período ({ultimo['Período']}): "
        f"RIPTE real=${ultimo['Monto en $ RIPTE']:,.2f} | "
        f"RIPTE simulado IPC=${ultimo['Monto en $ si los sueldos seguían al IPC']:,.2f} | "
        f"Diferencia={((ultimo['Monto en $ RIPTE'] / ultimo['Monto en $ si los sueldos seguían al IPC']) - 1) * 100:.1f}%"
    )

    return df_output


def main() -> None:
    """Pipeline principal: extrae datos, procesa y genera CSV."""
    logger.info("=" * 60)
    logger.info("Inicio del análisis RIPTE vs IPC Nacional General")
    logger.info("=" * 60)

    # 1. Extraer datos
    df_ripte = extract_ripte_from_pdf(RIPTE_PDF)
    df_ipc = extract_ipc_from_excel(IPC_XLS)

    # 2. Validación cruzada de rangos
    ripte_min, ripte_max = df_ripte["periodo"].min(), df_ripte["periodo"].max()
    ipc_min, ipc_max = df_ipc["periodo"].min(), df_ipc["periodo"].max()

    overlap_start = max(ripte_min, ipc_min)
    overlap_end = min(ripte_max, ipc_max)

    if overlap_start > overlap_end:
        raise ValueError(
            f"No hay overlap entre RIPTE ({ripte_min:%Y-%m} – {ripte_max:%Y-%m}) "
            f"e IPC ({ipc_min:%Y-%m} – {ipc_max:%Y-%m})"
        )

    logger.info(
        f"Overlap detectado: {overlap_start:%Y-%m} a {overlap_end:%Y-%m} "
        f"(se usará como base el período más antiguo: {overlap_start:%Y-%m})"
    )

    # 3. Generar CSV
    df_result = build_analysis_csv(df_ripte, df_ipc, OUTPUT_CSV)

    # 4. Preview
    print("\n" + "=" * 80)
    print("PREVIEW (primeros y últimos 5 registros)")
    print("=" * 80)
    print(df_result.head(5).to_string(index=False))
    print("...")
    print(df_result.tail(5).to_string(index=False))
    print(f"\nArchivo guardado en: {OUTPUT_CSV}")


if __name__ == "__main__":
    main()
