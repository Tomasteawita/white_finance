import os
import logging
from pathlib import Path
import pandas as pd

# Configurar logging para trazabilidad de auditoría (CNV)
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - [%(levelname)s] - %(message)s"
)
logger = logging.getLogger(__name__)

class ExecutionTransferenciasParaAhorrar:
    """
    ETL para extraer transferencias de la cuenta corriente histórica de inversiones.
    
    LÓGICA DE NEGOCIO:
    Filtra los movimientos de fondos (ingresos y egresos) del broker representados 
    por 'RECIBO DE COBRO' (ingreso de dinero) y 'ORDEN DE PAGO' (egreso de dinero). 
    Esta información es fundamental para determinar el capital neto aportado por el inversor 
    (flujo de fondos neto) y así poder calcular de manera precisa el retorno de la cartera 
    mediante el método de Valor Cuotaparte (VCP) o la Tasa Interna de Retorno (TIR), 
    separando los aportes de capital de la valorización puramente de mercado.
    """

    def __init__(self) -> None:
        # La raíz del proyecto está 4 niveles arriba del archivo actual:
        # scripts/pipelines/portfolio_visualization/execute_transferencias_para_ahorrar.py
        self.root_path: Path = Path(__file__).resolve().parents[3]
        
        self.path_cuenta_corriente_historico: Path = (
            self.root_path / "data" / "analytics" / "cuenta_corriente_historico.csv"
        )
        self.path_output_transferencias: Path = (
            self.root_path / "data" / "analytics" / "Transferencias Bull Market.csv"
        )

    def execute(self) -> None:
        """
        Ejecuta el proceso de extracción, filtrado y guardado de las transferencias.
        """
        logger.info(
            "Iniciando extracción de transferencias. Origen: %s",
            self.path_cuenta_corriente_historico
        )

        # Validación defensiva: Verificar existencia del archivo de origen
        if not self.path_cuenta_corriente_historico.exists():
            error_msg = f"Archivo de cuenta corriente no encontrado: {self.path_cuenta_corriente_historico}"
            logger.error(error_msg)
            raise FileNotFoundError(error_msg)

        try:
            df = pd.read_csv(self.path_cuenta_corriente_historico, sep=',')
        except Exception as e:
            logger.error("Error al leer el archivo de cuenta corriente histórica: %s", str(e))
            raise

        # Validación defensiva: Verificar que existan las columnas requeridas
        required_cols = ['Liquida', 'Comprobante', 'Importe']
        missing_cols = [col for col in required_cols if col not in df.columns]
        if missing_cols:
            error_msg = f"Faltan columnas requeridas en el archivo de cuenta corriente: {missing_cols}"
            logger.error(error_msg)
            raise ValueError(error_msg)

        df_filtrado = df[required_cols].copy()

        # Filtrar por los valores específicos en la columna "Comprobante"
        # 'RECIBO DE COBRO' y 'ORDEN DE PAGO' son el estándar en ALyCs para transferencias
        df_filtrado = df_filtrado[df_filtrado['Comprobante'].isin(['RECIBO DE COBRO', 'ORDEN DE PAGO'])]

        # Crear el directorio de salida si no existe
        output_dir = self.path_output_transferencias.parent
        output_dir.mkdir(parents=True, exist_ok=True)

        # Exportar a CSV
        try:
            df_filtrado.to_csv(self.path_output_transferencias, sep=',', index=False)
            logger.info(
                "Archivo de transferencias exportado exitosamente a %s. Total registros: %d",
                self.path_output_transferencias,
                len(df_filtrado)
            )
        except Exception as e:
            logger.error("Error al exportar archivo de transferencias: %s", str(e))
            raise

if __name__ == "__main__":
    ExecutionTransferenciasParaAhorrar().execute()