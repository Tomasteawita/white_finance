import os
import sys
from pathlib import Path

# Buscamos la raíz del proyecto dinámicamente (4 niveles arriba desde este archivo)
root_path = Path(__file__).resolve().parent.parent.parent.parent
sys.path.append(str(root_path / "scripts" / "layers"))

from portfolio_visualization.unify_accounts_db import UnifiedAccountPricer

def main():
    print("Iniciando la Dolarización Unificada de Cuentas Corrientes...")
    
    # Rutas dinámicas
    csv_pesos = str(root_path / "data" / "analytics" / "cuenta_corriente_historico.csv")
    csv_mep = str(root_path / "data" / "analytics" / "cuenta_corriente_dolares_historico.csv")
    csv_ccl = str(root_path / "data" / "analytics" / "cuenta_corriente_dolares_cable_historico.csv")
    
    output_path = str(root_path / "data" / "analytics" / "cuentas_unificadas_sorted.csv")

    # Inicializamos el orquestador
    pricer = UnifiedAccountPricer(
        csv_pesos=csv_pesos,
        csv_mep=csv_mep,
        csv_ccl=csv_ccl
    )

    # Generamos el Dataset Final en USD MEP
    df_cuentas_unificadas_usd = pricer.get_unified_accounts_usd()

    # Guardamos CSV con el baseline final
    df_cuentas_unificadas_usd.to_csv(output_path, index=False)
    
    print(f"\nProceso Finalizado. El output consolidado se guardó en:\n{output_path}")

if __name__ == "__main__":
    main()
