import os
import pandas as pd
from dotenv import load_dotenv

class UnifiedAccountPricer:
    """
    Clase que toma las 3 cuentas corrientes crudas (Pesos, MEP, Cable)
    y unifica sus registros en Dolares CCL (Cable) consultando el tipo de cambio
    institucional directamente desde PostgreSQL (earnings.ccl_mep).
    Además, transforma los CEDEARs a sus cantidades subyacentes equivalentes en EE.UU.
    y normaliza los tickers eliminando sufijos locales.
    """
    
    def __init__(self, csv_pesos, csv_mep, csv_ccl):
        self.csv_pesos = csv_pesos
        self.csv_mep = csv_mep
        self.csv_ccl = csv_ccl
        self.columns_needed = [
            'Liquida', 'Operado', 'Comprobante', 'Numero', 
            'Cantidad', 'Especie', 'Precio', 'Importe', 'Saldo', 'Referencia', 'Origen'
        ]
        
        # Diccionario unificado de Ratios
        self.ratios_cedear = {
            'KO': 5.0, 'SPY': 20.0, 'QQQ': 20.0, 'AAPL': 10.0,
            'GOOGL': 58.0, 'MSFT': 30.0, 'TSLA': 15.0, 'MELI': 120.0,
            'LLY': 56.0, 'META': 24.0, 'VIST': 3.0, 'AMZN': 144.0,
            'NVDA': 24.0, 'NFLX': 60.0, 'TLT': 1.0, 'SH': 8.0,
            'ARGT': 1.0, 'XLP': 1.0, 'SHY': 1.0, 'ADBE': 44.0,
            'ARKK': 10.0, 'ASML': 146.0, 'BBD': 1.0, 'BIOX': 1.0,
            'COIN': 27.0, 'ERIC': 2.0, 'HMY': 1.0, 'LAR': 1.0,
            'PAAS': 3.0, 'PSQ': 8.0, 'SAN': 0.25, 'UNH': 33.0, 'VALE': 2.0
        }
        
    def get_unified_accounts_usd(self):
        print("Cargando CSVs originales...")
        df_pesos = pd.read_csv(self.csv_pesos)
        df_mep = pd.read_csv(self.csv_mep)
        df_ccl = pd.read_csv(self.csv_ccl)

        
        
        df_pesos['Origen'] = 'ARS'
        df_mep['Origen'] = 'USD MEP'
        df_ccl['Origen'] = 'USD CCL'
        
        df_unified = pd.concat([df_pesos, df_mep, df_ccl], ignore_index=True)
        
        # Eliminamos duplicados reales
        df_unified = df_unified.sort_values(by=['Numero', 'Especie', 'Operado'], na_position='last')
        
        # Restaurar orden cronológico estricto (Liquida -> Operado)
        df_unified = df_unified.sort_values(by=['Liquida', 'Operado', 'Numero']).reset_index(drop=True)
        
        print(f"Dataset Dolarizado en CCL Exitosamente! Registros finales unívocos: {len(df_unified)}")
        return df_unified