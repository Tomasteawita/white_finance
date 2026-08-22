import os
import sys
import subprocess
import glob

def main():
    print("====================================================================")
    print("🚀 Iniciando Pipeline de Procesamiento Balanz")
    print("====================================================================")
    print("⚠️ IMPORTANTE:")
    print("Antes de ejecutar todo el proceso, revisa las cotizaciones historicas en Rava Bursátil y en la web de la CNV para descargar los archivos necesarios de fondos comunes de inversión.")
    
    confirm = input("\n¿Confirmas que ya has realizado este paso? (y/n): ").strip().lower()
    
    if confirm != 'y':
        print("Ejecución cancelada por el usuario.")
        sys.exit(0)
        
    base_path = r"c:\Users\tomas\white_finance"
    python_exe = os.path.join(base_path, "venv", "Scripts", "python.exe")
    
    if not os.path.exists(python_exe):
        print(f"Error: No se encontró el ejecutable de Python del entorno virtual en {python_exe}")
        sys.exit(1)

    print("\n--------------------------------------------------------------------")
    print("1. Ingesta manual de cotizaciones")
    print("--------------------------------------------------------------------")
    especies_input = input("Ingresa las especies a procesar separadas por coma (ej. S14G6, T15E7). Deja vacío para procesar todas: ").strip()
    
    cotizaciones_dir = os.path.join(base_path, "data", "analytics", "cotizaciones")
    archivos_cotizaciones = []
    
    if especies_input:
        especies = [esp.strip() for esp in especies_input.split(",")]
        for esp in especies:
            file_path = os.path.join(cotizaciones_dir, f"{esp} - Cotizaciones historicas.csv")
            if os.path.exists(file_path):
                archivos_cotizaciones.append(file_path)
            else:
                print(f"⚠️ Advertencia: No se encontró el archivo para {esp} en {file_path}")
    
    cmd_ingest = [python_exe, os.path.join(base_path, "scripts", "layers", "portfolio_visualization", "ingest_manual_quotes.py")]
    if archivos_cotizaciones:
        cmd_ingest.extend(archivos_cotizaciones)
        
    print("\nEjecutando ingest_manual_quotes.py...")
    subprocess.run(cmd_ingest, check=True, cwd=base_path)

    print("\n--------------------------------------------------------------------")
    print("2. Extracción FCI CNV")
    print("--------------------------------------------------------------------")
    print("Ejecutando extraction_fci_cnv.py...")
    cmd_fci = [python_exe, os.path.join(base_path, "scripts", "layers", "balanz", "extraction_fci_cnv.py")]
    subprocess.run(cmd_fci, check=True, cwd=base_path)

    print("\n--------------------------------------------------------------------")
    print("3. Procesamiento unificado de Cuentas Corrientes (Balanz)")
    print("--------------------------------------------------------------------")
    print("Ejecutando process_balanz_cuentas.py...")
    cmd_cuentas = [python_exe, os.path.join(base_path, "scripts", "layers", "balanz", "process_balanz_cuentas.py")]
    subprocess.run(cmd_cuentas, check=True, cwd=base_path)

    print("\n--------------------------------------------------------------------")
    print("4. Evolución de Portfolio por Cliente")
    print("--------------------------------------------------------------------")
    data_balanz_dir = os.path.join(base_path, "data", "balanz")
    
    if not os.path.exists(data_balanz_dir):
        print(f"Error: No se encontró el directorio {data_balanz_dir}")
        sys.exit(1)

    clients = [d for d in os.listdir(data_balanz_dir) if os.path.isdir(os.path.join(data_balanz_dir, d))]
    
    script_evolution = os.path.join(base_path, "scripts", "layers", "balanz", "client_portfolio_evolution.py")
    
    for client in clients:
        print(f"\n--- Generando evolución para el cliente: {client} ---")
        cmd_evolution = [python_exe, script_evolution, client]
        subprocess.run(cmd_evolution, check=True, cwd=base_path)

    print("\n====================================================================")
    print("✅ Pipeline ejecutado exitosamente.")
    print("====================================================================")

if __name__ == "__main__":
    main()
