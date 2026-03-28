import sys
import os
from pathlib import Path

# Configuración de rutas para importar desde el proyecto
root_path = Path(__file__).resolve().parent.parent.parent
if str(root_path) not in sys.path:
    sys.path.append(str(root_path))

# Ahora importamos usando rutas absolutas desde la raíz del proyecto
from scripts.pipelines.AWS.refresh_earnings import main as refresh_earnings
from scripts.pipelines.portfolio_visualization.execute_unification import main as execute_unification
from scripts.pipelines.portfolio_visualization.extraction_prices import ExtractionPipeline
from scripts.pipelines.portfolio_visualization.execute_evolucion_patrimonio import EvolucionHistoricaPatrimonio
from scripts.pipelines.portfolio_visualization.execute_transferencias_para_ahorrar import ExecutionTransferenciasParaAhorrar

class ExecuteAllPipelines:

    def __init__(self, partition_date: str):
        self.partition_date = partition_date
        self.extraction_pipeline = ExtractionPipeline()
        self.evolucion_historica_patrimonio = EvolucionHistoricaPatrimonio()
        self.execution_transferencias_para_ahorrar = ExecutionTransferenciasParaAhorrar()

    def execute_all_pipelines(self):
        print("--- Iniciando Refresh Earnings ---")
        refresh_earnings()
        
        print("\n--- Iniciando Unificación de Cuentas ---")
        execute_unification()
        
        print("\n--- Iniciando Extracción de Precios ---")
        self.extraction_pipeline.run_from_cuentas_corrientes()
        
        print("\n--- Iniciando Evolución de Patrimonio ---")
        self.evolucion_historica_patrimonio.run()
        
        print("\n--- Iniciando Transferencias para Ahorrar ---")
        self.execution_transferencias_para_ahorrar.execute()

if __name__ == "__main__":
    # Puedes pasar la fecha de partición si es necesaria para refresh_earnings
    # o manejarla internamente. Por ahora inicializamos con un placeholder.
    pipeline = ExecuteAllPipelines(partition_date="hoy")
    pipeline.execute_all_pipelines()


