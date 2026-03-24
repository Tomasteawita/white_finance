import os
import sys

# Agregamos la ruta del proyecto actual al path para importar correctamente
sys.path.append(r'c:\Users\tomas\white_finance\scripts\layers')
from portfolio_visualization.unify_accounts_db import UnifiedAccountPricer

def main():
    print("Iniciando la Dolarización Unificada de Cuentas Corrientes...")
    
    # Rutas absolutas para máxima seguridad en ejecución
    csv_pesos = r'c:\Users\tomas\white_finance\data\analytics\cuenta_corriente_historico.csv'
    csv_mep = r'c:\Users\tomas\white_finance\data\analytics\cuenta_corriente_dolares_historico.csv'
    csv_ccl = r'c:\Users\tomas\white_finance\data\analytics\cuenta_corriente_dolares_cable_historico.csv'
    
    output_path = r'c:\Users\tomas\white_finance\data\analytics\cuentas_unificadas_usd_sorted.csv'

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
