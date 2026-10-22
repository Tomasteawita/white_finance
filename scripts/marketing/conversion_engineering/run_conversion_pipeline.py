import argparse
import logging
import analisis_tasa_conversion
import extract_conversation_structures
import consolidate_winning_patterns

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
log = logging.getLogger(__name__)

def main():
    parser = argparse.ArgumentParser(description="Pipeline de Ingeniería de Conversión (Frontend Orchestrator)")
    parser.add_argument("--process_date", required=True, help="Fecha de la carpeta de procesamiento (ej. 20260912)")
    parser.add_argument("--start_date", required=True, help="Fecha inicio para filtrar conversaciones (ej. 2026-09-01)")
    parser.add_argument("--end_date", required=True, help="Fecha fin para filtrar conversaciones (ej. 2026-09-30)")
    
    args = parser.parse_args()
    
    log.info("Iniciando Pipeline de Análisis de Conversiones")
    log.info(f"Parametros -> Process Date: {args.process_date} | Ventana: {args.start_date} a {args.end_date}")
    
    # 1. Ejecutar el escaneo de embudo (crea las metricas y saca los IDs que convirtieron)
    analisis_tasa_conversion.run(args.process_date, args.start_date, args.end_date)
    
    # 2. Reconstruir los dialogos de esos IDs ganadores y mandarlos al LLM
    extract_conversation_structures.run(args.process_date)
    
    # 3. Clusterizar y exportar paretos en formato tabular para el front
    consolidate_winning_patterns.run()
    
    log.info("Pipeline completado exitosamente.")

if __name__ == "__main__":
    main()
