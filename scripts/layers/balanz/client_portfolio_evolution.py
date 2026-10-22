import os
import sys
import json
import logging
from typing import List, Optional
import numpy as np
import pandas as pd
from dotenv import load_dotenv
from sqlalchemy import create_engine, text

if hasattr(sys.stdout, "reconfigure"):
    try:
        sys.stdout.reconfigure(encoding="utf-8", errors="replace")
        sys.stderr.reconfigure(encoding="utf-8", errors="replace")
    except Exception:
        pass

class BalanzClientPortfolioEvolution:
    """
    Clase encargada de generar los holdings diarios y la evolución 
    histórica del patrimonio para clientes de Balanz, enfocado en FCIs.
    """
    def __init__(self, client_name: str = "ARCE_ZULMA_ELIZABET"):
        # Rutas y configuración
        self.base_path = os.path.normpath(os.path.join(os.path.dirname(os.path.abspath(__file__)), '../../../'))
        self.client_name = client_name
        load_dotenv(os.path.join(self.base_path, '.env'))
        
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
        
        # DB (para CCL y precios históricos)
        db_user = os.getenv("POSTGRE_USER", "postgres")
        db_pwd = os.getenv("POSTGRE_PASSWORD", "postgres")
        db_host = os.getenv("POSTGRE_HOST", "localhost")
        db_port = os.getenv("POSTGRE_PORT", "5432")
        db_name = os.getenv("POSTGRE_DB", "postgres")
        self.db_uri = f"postgresql+psycopg2://{db_user}:{db_pwd}@{db_host}:{db_port}/{db_name}"

        self.maps_fci = self.load_maps()
        self.cc_path = self._find_cc_file()
        self.ratios_cedear = self.fetch_cedear_ratios()

    def fetch_cedear_ratios(self) -> dict:
        engine = create_engine(self.db_uri)
        try:
            df_ratios = pd.read_sql("SELECT ticker, ratio FROM earnings.ratios_cedears", engine)
            return dict(zip(df_ratios['ticker'], df_ratios['ratio']))
        except Exception as e:
            self.logger.error(f"No se pudieron cargar los ratios: {e}")
            return {}

    def _find_cc_file(self) -> str:
        cc_file = os.path.join(self.cc_dir, 'cuenta_corriente_historico.csv')
        if not os.path.exists(cc_file):
            raise FileNotFoundError(f"No se encontró cuenta_corriente_historico.csv en {self.cc_dir}")
        return cc_file

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
        df_ccl = df_ccl.drop_duplicates(subset=['date']).set_index('date')['ccl']
        return df_ccl


    def _get_historical_prices(self, tickers: set) -> pd.DataFrame:
        """Obtiene precios históricos para las especies de boletos desde PostgreSQL."""
        if not tickers:
            return pd.DataFrame()
        engine = create_engine(self.db_uri)
        tickers_tuple = tuple(tickers)
        if len(tickers_tuple) == 1:
            query = text("SELECT date, ticker, close, source FROM earnings.historical_prices WHERE ticker = :t")
            df = pd.read_sql(query, engine, params={'t': tickers_tuple[0]})
        else:
            query = f"SELECT date, ticker, close, source FROM earnings.historical_prices WHERE ticker IN {tickers_tuple}"
            df = pd.read_sql(query, engine)

        # ENFOQUE B: Identificar tickers faltantes o desactualizados (> 5 días sin nuevos precios)
        faltantes = set(tickers)
        if not df.empty:
            df['date'] = pd.to_datetime(df['date'])
            hoy = pd.Timestamp.today().normalize()
            max_dates = df.groupby('ticker')['date'].max()
            
            for t in max_dates.index:
                dias_diff = (hoy - max_dates[t]).days
                # Si el último precio fue hace 5 días o menos (cubre fines de semana y feriados), está actualizado
                if dias_diff <= 5:
                    faltantes.discard(t)
                    
        if faltantes:
            self.logger.info(f"Tickers faltantes o desactualizados en DB: {faltantes}. Invocando ExtractionPipeline...")
            try:
                import sys
                import os
                pipelines_path = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "..", "pipelines", "portfolio_visualization"))
                if pipelines_path not in sys.path:
                    sys.path.append(pipelines_path)
                from extraction_prices import ExtractionPipeline
                
                pipeline = ExtractionPipeline()
                pipeline.run(list(faltantes))
                
                if len(tickers_tuple) == 1:
                    df = pd.read_sql(query, engine, params={'t': tickers_tuple[0]})
                else:
                    df = pd.read_sql(query, engine)
            except Exception as e:
                self.logger.error(f"Falla al ejecutar ExtractionPipeline: {e}")
            
        if df.empty:
            return pd.DataFrame()
            
        # Unir CCL para pesificar si la fuente es YFinance_USD
        df['date'] = pd.to_datetime(df['date'])
        ccl_series = self._get_ccl_series().reset_index()
        ccl_series['date'] = pd.to_datetime(ccl_series['date'])
        df = df.merge(ccl_series, on='date', how='left')
        df['ccl'] = df['ccl'].ffill().bfill()
        
        # Mapear el ratio del CEDEAR
        df['ratio'] = df['ticker'].map(self.ratios_cedear).fillna(1.0)
        
        # Calcular close_ars
        df['close_ars'] = np.where(
            df['source'] == 'YFinance_USD', 
            (df['close'] * df['ccl']) / df['ratio'], 
            df['close']
        )

        # Corrección: Bonos y Letras cotizan cada 100 nominales
        tickers_cada_100 = {"AE38", "AL30", "BPOC7", "GD30", "GD35", "S14G6", "T15E7", "TZX28", "AO27"}
        mask = df['ticker'].isin(tickers_cada_100)
        if mask.any():
            df.loc[mask, 'close_ars'] = df.loc[mask, 'close_ars'] / 100.0
        pivot = df.pivot(index='date', columns='ticker', values='close_ars')
        return pivot.ffill().bfill()

    def process_holdings(self):
        """Genera snapshots diarios de holdings leyendo la Cuenta Corriente."""
        df_cc = pd.read_csv(self.cc_path, sep="|")
        
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
        df_cc = df_cc.sort_values(by='Fecha', ascending=True, kind='mergesort').reset_index(drop=True)

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

        portfolio = {'Cash_ARS': 0.0, 'Cash_MEP': 0.0, 'Cash_CCL': 0.0}
        daily_snapshots = []
        boletos_tickers = set()
        
        for _, row in df_cc.iterrows():
            fecha = row['Fecha']
            descripcion = str(row['Descripcion']).strip()
            importe = float(row['Importe']) if pd.notna(row['Importe']) else 0.0
            saldo = float(row['Saldo']) if pd.notna(row['Saldo']) else 0.0
            cantidad = float(row['Cantidad']) if 'Cantidad' in row and pd.notna(row['Cantidad']) else 0.0

            flujo_ars = 0.0
            flujo_usd = 0.0

            # 1. Actualizar Cash y flujos dependiendo de la moneda
            moneda_str = str(row.get('moneda', '')).strip().lower()
            is_usd = ('lares' in moneda_str or 'lres' in moneda_str or 'd' in moneda_str or '7000' in moneda_str) and ('pesos' not in moneda_str)

            if descripcion.startswith("Recibo de Cobro"):
                if is_usd:
                    flujo_usd = abs(importe)
                else:
                    flujo_ars = abs(importe)
            elif descripcion.startswith("Comprobante de Pago"):
                if is_usd:
                    flujo_usd = -abs(importe)
                else:
                    flujo_ars = -abs(importe)

            if 'pesos' in moneda_str:
                portfolio['Cash_ARS'] = saldo
            elif '7000' in moneda_str:
                portfolio['Cash_CCL'] = saldo
            elif 'lares' in moneda_str or 'lres' in moneda_str or 'd' in moneda_str:
                portfolio['Cash_MEP'] = saldo
            else:
                portfolio['Cash_ARS'] = saldo

            # 2. Identificar si es operación de FCI o Boleto
            official_name = self._get_fci_name_from_description(descripcion)
            especie_boleto = None
            
            if not official_name and descripcion.startswith("Boleto"):
                parts = descripcion.split("/")
                if len(parts) >= 5:
                    especie_boleto = parts[4].strip()
                    especie_boleto = especie_boleto.replace('.BA', '').replace('.US', '')
            
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
                        if idx == -1:
                            # Si es anterior a la primera fecha disponible, usamos el precio más antiguo
                            idx = quotes_pivot.index.get_indexer([fecha], method='backfill')[0]
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
                    
            elif especie_boleto:
                if especie_boleto not in portfolio:
                    portfolio[especie_boleto] = 0.0
                    
                if "COMPRA" in descripcion:
                    portfolio[especie_boleto] += cantidad
                elif "VENTA" in descripcion:
                    portfolio[especie_boleto] -= cantidad
                    
                boletos_tickers.add(especie_boleto)

            elif descripcion.lower().startswith("amortiz") or descripcion.lower().startswith("amorctiz"):
                parts = descripcion.split("/")
                if len(parts) >= 2:
                    especie_amort = parts[1].strip()
                    especie_amort = especie_amort.replace('.BA', '').replace('.US', '')
                    # Parche temporal: Tratamiento como amortización bullet total (devuelve todo el capital y la tenencia queda en 0)
                    self.logger.info(
                        f"Amortización bullet detectada para {especie_amort} en fecha {fecha.strftime('%Y-%m-%d')}. "
                        f"Ajustando tenencia a 0.0."
                    )
                    portfolio[especie_amort] = 0.0
                    boletos_tickers.add(especie_amort)

            # Snapshot del estado después de la operación
            snapshot = portfolio.copy()
            snapshot['Operado'] = fecha
            snapshot['Flujo_ARS'] = flujo_ars
            snapshot['Flujo_USD'] = flujo_usd
            daily_snapshots.append(snapshot)

        # Si un día tiene múltiples operaciones, nos quedamos con el último snapshot (el cierre del día)
        df_snapshots = pd.DataFrame(daily_snapshots)
        daily_balances = df_snapshots.groupby('Operado').last().reset_index()
        
        # Flujos diarios acumulados (por si hay más de una operación el mismo día)
        if 'Flujo_ARS' in df_snapshots.columns and 'Flujo_USD' in df_snapshots.columns:
            flujo_diario = df_snapshots.groupby('Operado')[['Flujo_ARS', 'Flujo_USD']].sum().reset_index()
            daily_balances = daily_balances.drop(columns=['Flujo_ARS', 'Flujo_USD'], errors='ignore')
            daily_balances = daily_balances.merge(flujo_diario, on='Operado', how='left')
        else:
            daily_balances['Flujo_ARS'] = 0.0
            daily_balances['Flujo_USD'] = 0.0
        
        return daily_balances, quotes_pivot, boletos_tickers

    def generate_evolution(self):
        self.logger.info(f"Calculando holdings a partir de la cuenta corriente de {self.client_name}...")
        holdings_diarios, quotes_pivot, boletos_tickers = self.process_holdings()
        
        # Obtener precios de boletos e integrarlos
        if boletos_tickers:
            boletos_quotes = self._get_historical_prices(boletos_tickers)
            if not boletos_quotes.empty:
                quotes_pivot = quotes_pivot.join(boletos_quotes, how='outer').ffill().bfill()
        
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
        
        # --- NUEVO: Ajuste por Stock Split del CEDEAR SPY (Ratio 20 a 60, Split 3:1) ---
        # El split ocurrió aprox el 2026-06-01. Todas las tenencias acumuladas hasta ese momento se triplicaron.
        # Las operaciones de cuenta corriente posteriores ya venían con la nueva cotización y cantidad.
        if 'SPY' in holdings.columns:
            try:
                pre_split_spy = holdings.loc[:'2026-05-31', 'SPY'].iloc[-1]
                if pre_split_spy > 0:
                    holdings.loc['2026-06-01':, 'SPY'] += (pre_split_spy * 2)
                    print(f"DEBUG POST SPLIT: {holdings.loc['2026-09-18', 'SPY']}")
            except Exception as e:
                pass
        
        # Para los flujos no queremos ffill (los flujos solo ocurren en el día exacto)
        if 'Flujo_ARS' in holdings_diarios.columns:
            holdings['Flujo_ARS'] = holdings_diarios['Flujo_ARS'].reindex(idx, fill_value=0.0)
            holdings['Flujo_USD'] = holdings_diarios['Flujo_USD'].reindex(idx, fill_value=0.0)
        else:
            holdings['Flujo_ARS'] = 0.0
            holdings['Flujo_USD'] = 0.0

        # Cargar CCL
        serie_ccl = self._get_ccl_series().reindex(holdings.index).ffill().bfill()
        
        # Preparar dataframe consolidado
        df_consolidado = holdings.copy()
        df_consolidado['CCL'] = serie_ccl

        # Alinear la matriz de precios a las fechas del holding
        precios_matriz = quotes_pivot.reindex(holdings.index).ffill()
        
        self.logger.info("Aplicando valuación de cuotapartes y especies usando precios extraídos...")
        # Valorizar Activos (Nominales * Precio = Valor en ARS)
        no_activos = {'Cash_ARS', 'Cash_MEP', 'Cash_CCL', 'Flujo_ARS', 'Flujo_USD'}
        activos_names = [col for col in holdings.columns if col not in no_activos]
        for activo in activos_names:
            if activo in precios_matriz.columns:
                df_consolidado[activo + '_Val_ARS'] = df_consolidado[activo] * precios_matriz[activo]
            else:
                df_consolidado[activo + '_Val_ARS'] = 0.0
                
        # Total Patrimonio ARS = Cash + sum(Valor_Activo)
        val_columns = [col for col in df_consolidado.columns if col.endswith('_Val_ARS')]
        df_consolidado['Total_Activos_ARS'] = df_consolidado[val_columns].sum(axis=1)
        
        # Asumimos que el MEP y CCL son similares para el saldo en dolares. 
        # Multiplicamos Cash_MEP y Cash_CCL por el valor de CCL
        df_consolidado['Patrimonio_ARS'] = (
            df_consolidado['Cash_ARS'] + 
            (df_consolidado['Cash_MEP'] * df_consolidado['CCL']) + 
            (df_consolidado['Cash_CCL'] * df_consolidado['CCL']) +
            df_consolidado['Total_Activos_ARS']
        )
        
        # Total Patrimonio USD = Patrimonio_ARS / CCL
        df_consolidado['Patrimonio_USD'] = df_consolidado['Patrimonio_ARS'] / df_consolidado['CCL']
        
        # --- Evolución de Patrimonio Ahorrado (Ingresos vs Egresos) ---
        # LÓGICA DE NEGOCIO:
        # El patrimonio ahorrado refleja el capital acumulado que el cliente aportó (ingresos: Recibo de Cobro)
        # o retiró (egresos: Comprobante de Pago).
        # Los movimientos en USD se valorizan en ARS según el CCL del día de la operación,
        # garantizando consistencia monetaria directa con Patrimonio_ARS.
        flujo_neto_total_ars = df_consolidado['Flujo_ARS'] + (df_consolidado['Flujo_USD'] * df_consolidado['CCL'])
        df_consolidado['Patrimonio ahorrado'] = flujo_neto_total_ars.cumsum()
        
        # Limpiar columnas temporales de flujo
        df_consolidado = df_consolidado.drop(columns=['Flujo_ARS', 'Flujo_USD'], errors='ignore')

        # Formatear el output
        df_consolidado.index.name = 'Fecha'
        df_final = df_consolidado.reset_index()
        
        # Guardar CSV con los Holdings Diarios Detallados (nominales, valorizaciones y precios)
        output_holdings = os.path.join(self.reports_dir, f"holdings_diarios_{self.client_name.lower()}.csv")
        os.makedirs(os.path.dirname(output_holdings), exist_ok=True)
        df_final.to_csv(output_holdings, index=False)
        self.logger.info(f"Holdings diarios generados exitosamente. Archivo guardado en: {output_holdings}")
        
        columnas_finales = [
            'Fecha', 
            'Cash_ARS', 
            'Cash_MEP',
            'Cash_CCL',
            'Patrimonio_ARS', 
            'Patrimonio_USD', 
            'Patrimonio ahorrado'
        ]
        
        # Filtrar solo las columnas solicitadas (verificando que existan)
        columnas_presentes = [col for col in columnas_finales if col in df_final.columns]
        df_final = df_final[columnas_presentes]
        
        # Guardar CSV
        os.makedirs(os.path.dirname(self.output_evolution), exist_ok=True)
        df_final.to_csv(self.output_evolution, index=False)
        self.logger.info(f"Evolución generada exitosamente. Archivo guardado en: {self.output_evolution}")
        self.logger.info(f"Últimos 5 días de Patrimonio_USD: \n{df_final.tail().to_string()}")
        print(f"✅ [{self.client_name}] Holdings: {output_holdings} | Evolución: {self.output_evolution}")


def run_all_clients_evolution(selected_clients: Optional[List[str]] = None) -> None:
    """
    Ejecuta la generación de holdings diarios (`holdings_diarios_{cliente}.csv`) y
    evolución del patrimonio (`evolucion_{cliente}.csv`) para todos los clientes
    en `data/balanz/` (o el subconjunto indicado en `selected_clients`).
    """
    base_path = os.path.normpath(os.path.join(os.path.dirname(os.path.abspath(__file__)), '../../../'))
    data_balanz_dir = os.path.join(base_path, 'data', 'balanz')

    if not os.path.exists(data_balanz_dir):
        raise FileNotFoundError(f"No se encontró el directorio {data_balanz_dir}")

    all_clients = sorted([
        d for d in os.listdir(data_balanz_dir)
        if os.path.isdir(os.path.join(data_balanz_dir, d)) and not d.startswith('.')
    ])

    if selected_clients:
        selected_set = {c.strip() for c in selected_clients if c.strip()}
        clients = [c for c in all_clients if c in selected_set]
    else:
        clients = all_clients

    for client in clients:
        print(f"--- Generando holdings y evolución de patrimonio para: {client} ---")
        evolution = BalanzClientPortfolioEvolution(client_name=client)
        evolution.generate_evolution()


if __name__ == "__main__":
    if len(sys.argv) > 1 and sys.argv[1] == "--all":
        extra_clients = sys.argv[2:] if len(sys.argv) > 2 else None
        run_all_clients_evolution(selected_clients=extra_clients)
    else:
        # Podemos iterar sobre todos los clientes en un futuro, por ahora ejecutamos para ARCE_ZULMA_ELIZABET
        client_name = "ARCE_ZULMA_ELIZABET"
        
        # Manejo dinámico si se pasa por línea de comandos
        if len(sys.argv) > 1:
            client_name = sys.argv[1]
            
        try:
            evolution = BalanzClientPortfolioEvolution(client_name=client_name)
            evolution.generate_evolution()
        except Exception as e:
            print(f"CRITICAL ERROR: {e}")
            import traceback
            traceback.print_exc()

