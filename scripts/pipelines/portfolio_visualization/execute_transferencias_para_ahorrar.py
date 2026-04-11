import pandas as pd
import os

class ExecutionTransferenciasParaAhorrar:

    def __init__(self):
        self.path_cuenta_corriente_historico = '../../data/analytics/cuenta_corriente_historico.csv'

    def execute(self):
        df = pd.read_csv(self.path_cuenta_corriente_historico, sep=',')

        df_filtrado = df[['Liquida', 'Comprobante', 'Importe']]

        # Filtrar por los valores específicos en la columna "Comprobante"
        df_filtrado = df_filtrado[df_filtrado['Comprobante'].isin(['RECIBO DE COBRO', 'ORDEN DE PAGO'])]

        # Crear el directorio si no existe
        os.makedirs('../../data/analytics', exist_ok=True)

        # Exportar a CSV    
        df_filtrado.to_csv('../../data/analytics/Transferencias Bull Market.csv', sep=',', index=False)

        print(f"Archivo exportado exitosamente: {len(df_filtrado)} registros")

if __name__ == "__main__":
    ExecutionTransferenciasParaAhorrar().execute()