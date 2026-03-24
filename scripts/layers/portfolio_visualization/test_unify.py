import sys
import os

sys.path.append(r'c:\Users\tomas\white_finance\scripts\layers')
from portfolio_visualization.unify_accounts_db import UnifiedAccountPricer

def test_unify():
    pricer = UnifiedAccountPricer(
        csv_pesos=r'c:\Users\tomas\white_finance\data\analytics\cuenta_corriente_historico.csv',
        csv_mep=r'c:\Users\tomas\white_finance\data\analytics\cuenta_corriente_dolares_historico.csv',
        csv_ccl=r'c:\Users\tomas\white_finance\data\analytics\cuenta_corriente_dolares_cable_historico.csv'
    )
    
    df = pricer.get_unified_accounts_usd()
    print("Columnas:", df.columns.tolist())
    print("Total Filas:", len(df))
    print(df.head())

if __name__ == "__main__":
    test_unify()
