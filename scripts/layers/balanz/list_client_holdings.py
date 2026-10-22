import logging
import os
import sys
from datetime import datetime
from typing import Dict, List, Optional, Tuple

import numpy as np
import pandas as pd

# Garantizar salida UTF-8 en consolas Windows (evita UnicodeEncodeError con cp1252)
if hasattr(sys.stdout, "reconfigure"):
    try:
        sys.stdout.reconfigure(encoding="utf-8", errors="replace")
        sys.stderr.reconfigure(encoding="utf-8", errors="replace")
    except Exception:
        pass


class BalanzLatestHoldingsGenerator:
    """
    Generador del listado de activos en cartera por cliente al último día disponible
    en `holdings_diarios_{cliente}.csv`.

    QUÉ HACE:
    ---------
    Recorre los directorios de clientes en `data/balanz/<CLIENTE>/reports/`,
    lee el archivo `holdings_diarios_{cliente.lower()}.csv`, toma el último registro
    cronológico disponible y extrae todos los activos con tenencia positiva
    (descartando residuos numéricos de punto flotante `<= 1e-4` en cuotapartes rescatadas).

    POR QUÉ (LÓGICA DE NEGOCIO):
    ----------------------------
    1. Trazabilidad de Tenencias Actuales: Permite conocer de forma inmediata qué instrumentos
       financieros (FCIs, Bonos Soberanos, ONs, Acciones/CEDEARs) mantiene cada cliente al cierre
       más reciente, junto con sus nominales, valuación en ARS y USD (vía CCL) y ponderación.
    2. Ley de Potencia (Pareto 80/20): Ordena las posiciones de mayor a menor impacto patrimonial
       y calcula el peso acumulado para identificar rápidamente el subconjunto de activos que
       explica el 80% del riesgo/valor de la cartera.
    """

    META_COLUMNS = {
        "Fecha",
        "Cash_ARS",
        "Cash_MEP",
        "Cash_CCL",
        "CCL",
        "Total_Activos_ARS",
        "Patrimonio_ARS",
        "Patrimonio_USD",
        "Patrimonio ahorrado",
    }

    # Umbral mínimo de nominales/cuotapartes para filtrar residuos de punto flotante en rescates totales
    MIN_NOMINAL_THRESHOLD: float = 1e-2

    def __init__(self, base_path: Optional[str] = None) -> None:
        if base_path is None:
            self.base_path = os.path.normpath(
                os.path.join(os.path.dirname(os.path.abspath(__file__)), "../../../")
            )
        else:
            self.base_path = os.path.normpath(base_path)

        self.data_balanz_dir = os.path.join(self.base_path, "data", "balanz")
        self.logs_dir = os.path.join(self.base_path, "logs")
        os.makedirs(self.logs_dir, exist_ok=True)
        self._setup_logger()

    def _setup_logger(self) -> None:
        """Configura logging con trazabilidad de auditoría CNV."""
        self.logger = logging.getLogger("BalanzLatestHoldings")
        self.logger.setLevel(logging.INFO)

        if not self.logger.handlers:
            log_file = os.path.join(self.logs_dir, "balanz_pipelines.log")
            formatter = logging.Formatter(
                "%(asctime)s - [%(levelname)s] - %(name)s - %(message)s",
                datefmt="%Y-%m-%d %H:%M:%S",
            )

            fh = logging.FileHandler(log_file, encoding="utf-8")
            fh.setFormatter(formatter)
            self.logger.addHandler(fh)

            ch = logging.StreamHandler(sys.stdout)
            ch.setFormatter(formatter)
            self.logger.addHandler(ch)

    @staticmethod
    def _classify_asset_type(asset_name: str) -> str:
        """
        Clasifica heurísticamente el instrumento en FCI vs Especie/Ticker bursátil
        para facilitar el análisis de riesgo Barbell.
        """
        upper_name = asset_name.upper()
        if "BALANZ" in upper_name or "CLASE" in upper_name or "AHORRO" in upper_name or "CAPITAL" in upper_name:
            return "FCI"
        return "Especie / Ticker"

    def extract_client_latest_holdings(
        self, client_name: str, include_cash: bool = False
    ) -> Tuple[pd.DataFrame, Dict[str, float]]:
        """
        Extrae el listado de activos al último día disponible para un cliente específico.

        Args:
            client_name: Nombre de la carpeta del cliente dentro de `data/balanz/`.
            include_cash: Si es True, incluye filas de liquidez (`Cash_ARS`, `Cash_MEP`, `Cash_CCL`)
                          cuando su saldo sea distinto de cero. Por defecto False (solo activos).

        Returns:
            Tuple con:
            - DataFrame con el detalle de activos al último día disponible.
            - Diccionario con métricas resumen de la cartera a esa fecha.
        """
        reports_dir = os.path.join(self.data_balanz_dir, client_name, "reports")
        holdings_file = os.path.join(
            reports_dir, f"holdings_diarios_{client_name.lower()}.csv"
        )

        if not os.path.exists(holdings_file):
            raise FileNotFoundError(
                f"No existe el archivo de holdings para '{client_name}': {holdings_file}"
            )

        df = pd.read_csv(holdings_file)
        if df.empty:
            raise ValueError(
                f"El archivo de holdings para '{client_name}' está vacío: {holdings_file}"
            )

        # Garantizar orden cronológico y tomar la última fila disponible
        df["Fecha_dt"] = pd.to_datetime(df["Fecha"], errors="coerce")
        df = df.sort_values("Fecha_dt").reset_index(drop=True)
        last_row = df.iloc[-1]

        last_date = str(last_row["Fecha"])
        ccl = float(last_row.get("CCL", 0.0) or 0.0)
        total_activos_ars = float(last_row.get("Total_Activos_ARS", 0.0) or 0.0)
        patrimonio_ars = float(last_row.get("Patrimonio_ARS", 0.0) or 0.0)
        patrimonio_usd = float(last_row.get("Patrimonio_USD", 0.0) or 0.0)
        cash_ars = float(last_row.get("Cash_ARS", 0.0) or 0.0)
        cash_mep = float(last_row.get("Cash_MEP", 0.0) or 0.0)
        cash_ccl = float(last_row.get("Cash_CCL", 0.0) or 0.0)

        # Identificar columnas de nominales de activos
        asset_columns = [
            col
            for col in df.columns
            if col not in self.META_COLUMNS
            and col != "Fecha_dt"
            and not col.endswith("_Val_ARS")
        ]

        rows: List[Dict[str, object]] = []
        for asset in asset_columns:
            raw_nominal = last_row.get(asset, 0.0)
            nominales = float(raw_nominal) if pd.notna(raw_nominal) else 0.0

            val_col = f"{asset}_Val_ARS"
            raw_val_ars = last_row.get(val_col, 0.0)
            val_ars = float(raw_val_ars) if pd.notna(raw_val_ars) else 0.0

            # Filtrar activos con tenencia efectiva al último día disponible
            if abs(nominales) <= self.MIN_NOMINAL_THRESHOLD:
                continue

            precio_impl_ars = (val_ars / nominales) if abs(nominales) > 0 else 0.0
            val_usd = (val_ars / ccl) if ccl > 0 else 0.0
            peso_activos_pct = (
                (val_ars / total_activos_ars * 100.0) if total_activos_ars > 0 else 0.0
            )
            peso_patrimonio_pct = (
                (val_ars / patrimonio_ars * 100.0) if patrimonio_ars > 0 else 0.0
            )

            rows.append(
                {
                    "Cliente": client_name,
                    "Fecha_Ultimo_Dia": last_date,
                    "Activo": asset,
                    "Tipo_Activo": self._classify_asset_type(asset),
                    "Nominales": round(nominales, 4),
                    "Precio_Unitario_ARS": round(precio_impl_ars, 4),
                    "Valuacion_ARS": round(val_ars, 2),
                    "Valuacion_USD": round(val_usd, 2),
                    "CCL_Ref": round(ccl, 2),
                    "Peso_Sobre_Activos_Pct": round(peso_activos_pct, 2),
                    "Peso_Sobre_Patrimonio_Pct": round(peso_patrimonio_pct, 2),
                }
            )

        if include_cash:
            cash_items = [
                ("Cash_ARS", cash_ars, cash_ars, (cash_ars / ccl) if ccl > 0 else 0.0),
                ("Cash_MEP", cash_mep, cash_mep * ccl, cash_mep),
                ("Cash_CCL", cash_ccl, cash_ccl * ccl, cash_ccl),
            ]
            for cash_name, cash_nom, cash_val_ars, cash_val_usd in cash_items:
                if abs(cash_val_usd) >= 0.01:
                    rows.append(
                        {
                            "Cliente": client_name,
                            "Fecha_Ultimo_Dia": last_date,
                            "Activo": cash_name,
                            "Tipo_Activo": "Liquidez",
                            "Nominales": round(cash_nom, 2),
                            "Precio_Unitario_ARS": round(
                                (cash_val_ars / cash_nom) if cash_nom != 0 else 1.0, 4
                            ),
                            "Valuacion_ARS": round(cash_val_ars, 2),
                            "Valuacion_USD": round(cash_val_usd, 2),
                            "CCL_Ref": round(ccl, 2),
                            "Peso_Sobre_Activos_Pct": 0.0,
                            "Peso_Sobre_Patrimonio_Pct": round(
                                (cash_val_ars / patrimonio_ars * 100.0)
                                if patrimonio_ars > 0
                                else 0.0,
                                2,
                            ),
                        }
                    )

        df_assets = pd.DataFrame(rows)
        if not df_assets.empty:
            df_assets = df_assets.sort_values(
                by=["Valuacion_ARS", "Nominales"], ascending=[False, False]
            ).reset_index(drop=True)
            df_assets["Peso_Acumulado_Activos_Pct"] = (
                df_assets["Peso_Sobre_Activos_Pct"].cumsum().round(2)
            )
            # Pareto 80/20: marcar los activos principales que concentran el 80% de la cartera
            df_assets["Pareto_Core_80"] = np.where(
                df_assets["Peso_Acumulado_Activos_Pct"].shift(1, fill_value=0.0) < 80.0,
                "SI",
                "NO",
            )
        else:
            df_assets = pd.DataFrame(
                columns=[
                    "Cliente",
                    "Fecha_Ultimo_Dia",
                    "Activo",
                    "Tipo_Activo",
                    "Nominales",
                    "Precio_Unitario_ARS",
                    "Valuacion_ARS",
                    "Valuacion_USD",
                    "CCL_Ref",
                    "Peso_Sobre_Activos_Pct",
                    "Peso_Sobre_Patrimonio_Pct",
                    "Peso_Acumulado_Activos_Pct",
                    "Pareto_Core_80",
                ]
            )

        # Guardar CSV individual en la carpeta reports del cliente
        client_output_csv = os.path.join(
            reports_dir, f"activos_ultimo_dia_{client_name.lower()}.csv"
        )
        df_assets.to_csv(client_output_csv, index=False)

        summary_metrics = {
            "Cliente": client_name,
            "Fecha_Ultimo_Dia": last_date,
            "Cantidad_Activos": int(len(df_assets)),
            "Total_Activos_ARS": round(total_activos_ars, 2),
            "Total_Activos_USD": round((total_activos_ars / ccl) if ccl > 0 else 0.0, 2),
            "Cash_ARS": round(cash_ars, 2),
            "Cash_MEP": round(cash_mep, 2),
            "Cash_CCL": round(cash_ccl, 2),
            "Patrimonio_ARS": round(patrimonio_ars, 2),
            "Patrimonio_USD": round(patrimonio_usd, 2),
            "CCL": round(ccl, 2),
            "Output_CSV": client_output_csv,
        }

        self.logger.info(
            f"[{client_name}] Último día: {last_date} | Activos en cartera: {len(df_assets)} | "
            f"Total Activos USD: {summary_metrics['Total_Activos_USD']:,.2f} | "
            f"Guardado en: {client_output_csv}"
        )

        return df_assets, summary_metrics

    def generate_all_clients_latest_holdings(
        self,
        selected_clients: Optional[List[str]] = None,
        include_cash: bool = False,
    ) -> pd.DataFrame:
        """
        Genera el listado de activos al último día disponible para todos los clientes
        en `data/balanz/` y guarda un archivo consolidado en:
        `data/balanz/activos_por_cliente_ultimo_dia.csv`.
        """
        if not os.path.exists(self.data_balanz_dir):
            raise FileNotFoundError(
                f"No se encontró el directorio de clientes: {self.data_balanz_dir}"
            )

        all_clients = sorted(
            [
                d
                for d in os.listdir(self.data_balanz_dir)
                if os.path.isdir(os.path.join(self.data_balanz_dir, d))
                and not d.startswith(".")
            ]
        )

        if selected_clients:
            selected_set = {c.strip() for c in selected_clients if c.strip()}
            clients = [c for c in all_clients if c in selected_set]
        else:
            clients = all_clients

        all_assets_dfs: List[pd.DataFrame] = []
        summaries: List[Dict[str, object]] = []

        for client in clients:
            try:
                df_client, summary = self.extract_client_latest_holdings(
                    client_name=client, include_cash=include_cash
                )
                summaries.append(summary)
                if not df_client.empty:
                    all_assets_dfs.append(df_client)
            except Exception as exc:
                self.logger.error(
                    f"Error al extraer activos del último día para '{client}': {exc}"
                )

        if all_assets_dfs:
            df_consolidated = pd.concat(all_assets_dfs, ignore_index=True)
        else:
            df_consolidated = pd.DataFrame(
                columns=[
                    "Cliente",
                    "Fecha_Ultimo_Dia",
                    "Activo",
                    "Tipo_Activo",
                    "Nominales",
                    "Precio_Unitario_ARS",
                    "Valuacion_ARS",
                    "Valuacion_USD",
                    "CCL_Ref",
                    "Peso_Sobre_Activos_Pct",
                    "Peso_Sobre_Patrimonio_Pct",
                    "Peso_Acumulado_Activos_Pct",
                    "Pareto_Core_80",
                ]
            )

        consolidated_csv = os.path.join(
            self.data_balanz_dir, "activos_por_cliente_ultimo_dia.csv"
        )
        df_consolidated.to_csv(consolidated_csv, index=False)
        self.logger.info(
            f"✅ Consolidado de activos al último día guardado en: {consolidated_csv}"
        )

        # Imprimir reporte estructurado por consola
        print("\n" + "=" * 95)
        print("LISTADO DE ACTIVOS POR CLIENTE AL ÚLTIMO DÍA DISPONIBLE EN HOLDINGS")
        print("=" * 95)
        for summary in summaries:
            client = str(summary["Cliente"])
            fecha = str(summary["Fecha_Ultimo_Dia"])
            cant = int(summary["Cantidad_Activos"])
            tot_usd = float(summary["Total_Activos_USD"])
            pat_usd = float(summary["Patrimonio_USD"])
            print(
                f"\n▶ Cliente: {client} | Fecha: {fecha} | Activos: {cant} | "
                f"Valuación Activos USD: ${tot_usd:,.2f} | Patrimonio Total USD: ${pat_usd:,.2f}"
            )
            client_rows = df_consolidated[df_consolidated["Cliente"] == client]
            if client_rows.empty:
                print("   (Sin activos en cartera al último día disponible - 100% Liquidez / Sin saldo)")
            else:
                display_cols = [
                    "Activo",
                    "Tipo_Activo",
                    "Nominales",
                    "Precio_Unitario_ARS",
                    "Valuacion_ARS",
                    "Valuacion_USD",
                    "Peso_Sobre_Activos_Pct",
                    "Pareto_Core_80",
                ]
                print(client_rows[display_cols].to_string(index=False))
        print("\n" + "=" * 95)
        print(f"Archivo consolidado general: {consolidated_csv}")
        print("=" * 95 + "\n")

        return df_consolidated


def run_all_clients_latest_holdings(
    selected_clients: Optional[List[str]] = None,
    include_cash: bool = False,
) -> pd.DataFrame:
    """Función helper para invocación desde pipelines externos."""
    generator = BalanzLatestHoldingsGenerator()
    return generator.generate_all_clients_latest_holdings(
        selected_clients=selected_clients, include_cash=include_cash
    )


if __name__ == "__main__":
    extra_clients = [arg for arg in sys.argv[1:] if not arg.startswith("--")]
    include_cash_flag = "--include-cash" in sys.argv
    run_all_clients_latest_holdings(
        selected_clients=extra_clients if extra_clients else None,
        include_cash=include_cash_flag,
    )
