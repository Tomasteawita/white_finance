import os
import sys
import json
import logging
import numpy as np
import pandas as pd
from sqlalchemy import create_engine

class BalanzClientPortfolioEvolution:
    """
    Clase encargada de generar los holdings diarios y la evolución 
    histórica del patrimonio para clientes de Balanz, enfocado en FCIs.
    """
    def __init__(self, client_name: str = "ARCE_ZULMA_ELIZABET"):
        # Rutas y configuración
        self.base_path = os.path.normpath(os.path.join(os.path.dirname(os.path.abspath(__file__)), '../../../'))
        self.client_name = client_name
        
        # Archivos de entrada generales
        self.fci_quotes_path = os.path.join(self.base_path, 'data/analytics/cotizaciones/fci_quotes_historico.csv')
        
        # Directorios específicos del cliente
        self.client_dir = os.path.join(self.base_path, 'data/balanz', self.client_name)
        self.map_fci_path = os.path.join(self.client_dir, 'maps_fci.json')
        self.cc_dir = os.path.join(self.client_dir, 'Cuenta Corriente')
        self.reports_dir = os.path.join(self.client_dir, 'reports')
        
        # Output file
        output_filename = f"evolucion_{self.client_name.lower()}.csv"
        self.output_evolution = os.path.join(self.reports_dir, output_filename)
        
        # Logging config
        self.log_dir = os.path.join(self.base_path, 'logs')
        os.makedirs(self.log_dir, exist_ok=True)
        logging.basicConfig(
            filename=os.path.join(self.log_dir, 'balanz_pipelines.log'),
            level=logging.INFO,
            format='%(asctime)s - %(levelname)s - %(name)s - %(message)s',
            datefmt='%Y-%m-%d %H:%M:%S'
        )
        self.logger = logging.getLogger(f'PortfolioEvolution_{self.client_name}')
        
        # DB (para CCL)
        self.db_uri = "postgresql://postgres:postgres@localhost:5432/postgres"

        self.maps_fci = self.load_maps()
        self.cc_path = self._find_cc_file()

    def _find_cc_file(self) -> str:
        import glob
        pattern = os.path.join(self.cc_dir, '*.xlsx')
        files = glob.glob(pattern)
        if not files:
            raise FileNotFoundError(f"No se encontró ningún archivo de Cuenta Corriente en {self.cc_dir}")
        # Asumimos que toma el primero/único archivo disponible
        return files[0]

    def load_maps(self) -> dict:
        try:
            with open(self.map_fci_path, 'r', encoding='utf-8') as f:
                return json.load(f)
        except Exception as e:
            self.logger.error(f"Error cargando maps_fci.json en {self.map_fci_path}: {e}")
            return {}

    def _get_fci_name_from_description(self, descripcion: str) -> str:
        """Busca si alguna de las keys de maps_fci está en la descripción de la transacción."""
        if not isinstance(descripcion, str):
            return None
        for balanz_name, official_name in self.maps_fci.items():
            if balanz_name in descripcion:
                return official_name
        return None

    def _get_ccl_series(self) -> pd.Series:
        """Obtiene la serie histórica de CCL desde PostgreSQL."""
        engine = create_engine(self.db_uri)
        query = "SELECT date, ccl FROM earnings.ccl_mep"
        df_ccl = pd.read_sql(query, engine)
        df_ccl['date'] = pd.to_datetime(df_ccl['date'])
        # Retornar serie con date como índice
        df_ccl = df_ccl.drop_duplicates(subset=['date']).set_index('date')['ccl']
        return df_ccl

    def process_holdings(self):
        """Genera snapshots diarios de holdings leyendo la Cuenta Corriente."""
        df_cc = pd.read_excel(self.cc_path, sheet_name=0)
        
        # Validar columnas
        if 'Fecha' not in df_cc.columns or 'Descripcin' not in df_cc.columns:
            # Corrección de caracteres raros por encoding del excel original de balanz
            desc_col = [c for c in df_cc.columns if 'Descripc' in c][0]
            df_cc = df_cc.rename(columns={desc_col: 'Descripcion'})
        else:
            df_cc = df_cc.rename(columns={'Descripcin': 'Descripcion'})
            
        df_cc['Fecha'] = pd.to_datetime(df_cc['Fecha'])
        # Balanz exports newest first; reverse it so daily operations are strictly chronological
        df_cc = df_cc.iloc[::-1]
        df_cc = df_cc.sort_values(by='Fecha', kind='mergesort').reset_index(drop=True)

        # Cargar cotizaciones históricas de FCI extraídas por extraction_fci_cnv.py
        try:
            df_quotes = pd.read_csv(self.fci_quotes_path)
            df_quotes['date'] = pd.to_datetime(df_quotes['date'])
            # Pivot para acceso rápido al precio por ticker y fecha
            quotes_pivot = df_quotes.pivot(index='date', columns='ticker', values='close_ars')
            # Forward fill por si falta el precio en la fecha exacta
            quotes_pivot = quotes_pivot.ffill().bfill()
        except Exception as e:
            self.logger.warning(f"Error cargando las cotizaciones de FCIs: {e}. Se usará dataframe vacío.")
            # Simulamos un pivot vacío
            quotes_pivot = pd.DataFrame()

        portfolio = {'Cash_ARS': 0.0}
        daily_snapshots = []
        
        for _, row in df_cc.iterrows():
            fecha = row['Fecha']
            descripcion = row['Descripcion']
            importe = float(row['Importe']) if pd.notna(row['Importe']) else 0.0
            saldo = float(row['Saldo']) if pd.notna(row['Saldo']) else 0.0

            # 1. Actualizar Cash (el saldo que reporta Balanz es el Cash_ARS del cliente)
            portfolio['Cash_ARS'] = saldo

            # 2. Identificar si es operación de FCI
            official_name = self._get_fci_name_from_description(descripcion)
            
            if official_name:
                # Si no existe en el portfolio, inicializamos en 0
                if official_name not in portfolio:
                    portfolio[official_name] = 0.0
                
                # Obtener el precio del FCI para esta fecha
                precio_fci = None
                if official_name in quotes_pivot.columns:
                    try:
                        # Buscar el precio más cercano anterior o igual a esta fecha
                        idx = quotes_pivot.index.get_indexer([fecha], method='pad')[0]
                        if idx != -1:
                            precio_fci = quotes_pivot.iloc[idx][official_name]
                    except KeyError:
                        pass
                
                if precio_fci and not np.isnan(precio_fci) and precio_fci > 0:
                    # Suscripción = Importe negativo (salida de caja). Rescate = Importe positivo.
                    # Cantidad comprada = (-Importe) / Precio
                    cantidad_operada = -importe / precio_fci
                    portfolio[official_name] += cantidad_operada
                else:
                    self.logger.warning(f"Precio de {official_name} no encontrado en {fecha.strftime('%Y-%m-%d')} para importe {importe}. Operación no computada en nominales.")

            # Snapshot del estado después de la operación
            snapshot = portfolio.copy()
            snapshot['Operado'] = fecha
            daily_snapshots.append(snapshot)

        # Si un día tiene múltiples operaciones, nos quedamos con el último snapshot (el cierre del día)
        df_snapshots = pd.DataFrame(daily_snapshots)
        daily_balances = df_snapshots.groupby('Operado').last().reset_index()
        
        return daily_balances, quotes_pivot

    def generate_evolution(self):
        self.logger.info(f"Calculando holdings a partir de la cuenta corriente de {self.client_name}...")
        holdings_diarios, quotes_pivot = self.process_holdings()
        
        if holdings_diarios.empty:
            self.logger.warning("No se encontraron holdings.")
            return
            
        holdings_diarios.set_index('Operado', inplace=True)
        
        # Forward fill desde la primera operación hasta el último precio disponible o hasta hoy
        end_date = quotes_pivot.index.max() if not quotes_pivot.empty else pd.Timestamp.today()
        # Si la fecha máxima de la cuenta corriente es mayor, usar esa
        if holdings_diarios.index.max() > end_date:
            end_date = holdings_diarios.index.max()
            
        idx = pd.date_range(holdings_diarios.index.min(), end_date)
        holdings = holdings_diarios.reindex(idx, method='ffill').fillna(0)

        # Cargar CCL
        serie_ccl = self._get_ccl_series().reindex(holdings.index).ffill().bfill()
        
        # Preparar dataframe consolidado
        df_consolidado = holdings.copy()
        df_consolidado['CCL'] = serie_ccl

        # Alinear la matriz de precios a las fechas del holding
        precios_matriz = quotes_pivot.reindex(holdings.index).ffill()
        
        self.logger.info("Aplicando valuación de cuotapartes usando precios extraídos...")
        # Valorizar FCIs (Nominales * Precio = Valor en ARS)
        fci_names = [col for col in holdings.columns if col != 'Cash_ARS']
        for fci in fci_names:
            if fci in precios_matriz.columns:
                df_consolidado[fci + '_Val_ARS'] = df_consolidado[fci] * precios_matriz[fci]
            else:
                df_consolidado[fci + '_Val_ARS'] = 0.0
                
        # Total Patrimonio ARS = Cash + sum(Valor_FCI)
        val_columns = [col for col in df_consolidado.columns if col.endswith('_Val_ARS')]
        df_consolidado['Total_FCIs_ARS'] = df_consolidado[val_columns].sum(axis=1)
        df_consolidado['Patrimonio_ARS'] = df_consolidado['Cash_ARS'] + df_consolidado['Total_FCIs_ARS']
        
        # Total Patrimonio USD = Patrimonio_ARS / CCL
        df_consolidado['Patrimonio_USD'] = df_consolidado['Patrimonio_ARS'] / df_consolidado['CCL']
        
        # Formatear el output
        df_consolidado.index.name = 'Fecha'
        df_final = df_consolidado.reset_index()
        
        # Guardar CSV
        os.makedirs(os.path.dirname(self.output_evolution), exist_ok=True)
        df_final.to_csv(self.output_evolution, index=False)
        self.logger.info(f"Evolución generada exitosamente. Archivo guardado en: {self.output_evolution}")
        
        self.logger.info(f"Últimos 5 días de Patrimonio_USD: \n{df_final[['Fecha', 'Cash_ARS', 'Total_FCIs_ARS', 'Patrimonio_ARS', 'CCL', 'Patrimonio_USD']].tail().to_string()}")

if __name__ == "__main__":
    # Podemos iterar sobre todos los clientes en un futuro, por ahora ejecutamos para ARCE_ZULMA_ELIZABET
    client_name = "ARCE_ZULMA_ELIZABET"
    
    # Manejo dinámico si se pasa por línea de comandos
    if len(sys.argv) > 1:
        client_name = sys.argv[1]
        
    try:
        evolution = BalanzClientPortfolioEvolution(client_name=client_name)
        evolution.generate_evolution()
    except Exception as e:
        logging.error(f"Error crítico procesando el cliente {client_name}: {e}")
