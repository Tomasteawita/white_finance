import pandas as pd

class CuentasCorrientesUnificacion:
    """
    Clase para unificar mi cuenta corriente en pesos, dolares mep y dolares ccl en
    una sola cuenta corriente ordenada por la columna Operado.
    Los saldos e importes finales se van a ver reflejados en dolares CCL
    """

    def __init__(self, csv_path_pesos, csv_path_usd_mep, csv_path_usd_ccl):
        self.csv_path_pesos = csv_path_pesos
        self.csv_path_usd_mep = csv_path_usd_mep
        self.csv_path_usd_ccl = csv_path_usd_ccl
        self.columns_needed = [
            'Liquida', 'Operado', 'Comprobante', 'Numero', 
            'Cantidad', 'Especie', 'Precio', 'Importe', 'Saldo', 'Referencia', 'Origen'
        ]
    
    def read_data(self):
        print("Cargando datos de cuenta corriente en pesos...")
        df_pesos = pd.read_csv(self.csv_path_pesos)
        print("Cargando datos de cuenta corriente en dólares MEP...")
        df_usd_mep = pd.read_csv(self.csv_path_usd_mep)
        print("Cargando datos de cuenta corriente en dólares CCL...")
        df_usd_ccl = pd.read_csv(self.csv_path_usd_ccl)

        return (df_pesos, df_usd_mep, df_usd_ccl)
    
    def format_cuenta_corriente_mep(self, df_usd_mep):
        """
        Pongo los valores de las columnas Importe_USD_CCL y Saldo_USD_CCL
        en las columnas Importe y Saldo
        """
        df_usd_mep['Importe'] = df_usd_mep['Importe_USD_CCL']
        df_usd_mep['Saldo'] = df_usd_mep['Saldo_USD_CCL']
        # elimino las coumnas Importe_USD_CCL y Saldo_USD_CCL
        df_usd_mep = df_usd_mep.drop(columns=['Importe_USD_CCL', 'Saldo_USD_CCL'])

        df_usd_mep = df_usd_mep[self.columns_needed]
        return df_usd_mep
    
    def format_cuenta_corriente_pesos(self, df_pesos):
        """
        Pongo los valores de las columnas Importe_USD y Saldo_USD
        en las columnas Importe y Saldo
        """
        df_pesos['Importe'] = df_pesos['Importe_USD']
        df_pesos['Saldo'] = df_pesos['Saldo_USD']
        # elimino las coumnas Importe_USD_CCL y Saldo_USD_CCL
        df_pesos = df_pesos.drop(columns=['Importe_USD', 'Saldo_USD'])
        df_pesos = df_pesos[self.columns_needed]
        return df_pesos
    
    def unify_accounts(self):
        df_pesos, df_usd_mep, df_usd_ccl = self.read_data()

        # le agrego una columna a cada cuenta corriente para identificar su origen
        df_pesos['Origen'] = 'ARS'
        df_usd_mep['Origen'] = 'USD MEP'
        df_usd_ccl['Origen'] = 'USD CCL'

        df_usd_mep = self.format_cuenta_corriente_mep(df_usd_mep)
        df_pesos = self.format_cuenta_corriente_pesos(df_pesos)

        print("Unificando cuentas corrientes...")
        df_unified = pd.concat([df_pesos, df_usd_mep, df_usd_ccl], ignore_index=True)

        print("Ordenando por columna Operado...")
        df_unified['Operado'] = pd.to_datetime(df_unified['Operado'], format='%Y-%m-%d')
        df_unified = df_unified.sort_values(by='Operado').reset_index(drop=True)

        return df_unified

        