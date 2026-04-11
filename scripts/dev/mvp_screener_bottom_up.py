import os
import logging
import yfinance as yf
import pandas as pd
import numpy as np
from sqlalchemy import create_engine, text
from sqlalchemy.engine import Engine
from typing import List, Dict, Optional
from dotenv import load_dotenv

# Configuración de Logging para trazabilidad
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

def get_db_engine() -> Engine:
    """
    Inicializa y retorna la conexión a PostgreSQL usando variables de entorno.
    POR QUÉ: Evita credenciales hardcodeadas, garantizando seguridad y portabilidad.
    """
    load_dotenv()
    db_user = os.getenv("POSTGRE_USER", "postgres")
    db_pass = os.getenv("POSTGRE_PASSWORD", "admin")
    db_host = os.getenv("POSTGRE_HOST", "localhost")
    db_port = os.getenv("POSTGRE_PORT", "5432")
    db_name = os.getenv("POSTGRE_DB", "postgres")
    
    db_url = f"postgresql://{db_user}:{db_pass}@{db_host}:{db_port}/{db_name}"
    return create_engine(db_url)

def init_db_schema(engine: Engine) -> None:
    """
    Crea el esquema 'raw_fundamentals' y la tabla de caché si no existen.
    POR QUÉ: Garantiza la inmutabilidad y persistencia de los datos descargados,
    evitando llamadas redundantes a la API externa (YFinance).
    """
    create_schema_query = "CREATE SCHEMA IF NOT EXISTS raw_fundamentals;"
    create_table_query = """
    CREATE TABLE IF NOT EXISTS raw_fundamentals.yfinance_cache (
        ticker VARCHAR(10),
        statement_type VARCHAR(20),
        metric VARCHAR(100),
        as_of_date DATE,
        value NUMERIC,
        ingestion_timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        PRIMARY KEY (ticker, statement_type, metric, as_of_date)
    );
    """
    try:
        with engine.begin() as conn:
            conn.execute(text(create_schema_query))
            conn.execute(text(create_table_query))
            logger.info("Esquema 'raw_fundamentals' y tabla de caché inicializados/verificados correctamente.")
    except Exception as e:
        logger.error(f"Error al inicializar el esquema de base de datos: {e}")
        raise

def fetch_and_cache_financials(ticker: str, engine: Engine) -> None:
    """
    Descarga los balances de YFinance, los transforma a formato Long (Tidy Data) 
    y los guarda en PostgreSQL. Solo inserta datos nuevos.
    POR QUÉ: Transforma las matrices ruidosas de YFinance en un formato columnar
    auditable y listo para el análisis cuantitativo.
    """
    stock = yf.Ticker(ticker)
    
    # Diccionario de los estados financieros a descargar
    statements = {
        'income_statement': stock.financials,
        'balance_sheet': stock.balance_sheet,
        'cash_flow': stock.cashflow
    }
    
    for stmt_type, df in statements.items():
        if df is None or df.empty:
            logger.warning(f"No hay datos de {stmt_type} para {ticker}.")
            continue
            
        # Transformar matriz ancha a formato largo (melt)
        df_reset = df.reset_index().rename(columns={'index': 'metric'})
        df_reset.columns = df_reset.columns.astype(str)
        df_melted = df_reset.melt(id_vars=['metric'], var_name='as_of_date', value_name='value')
        
        # Limpieza de datos
        df_melted['ticker'] = ticker
        df_melted['statement_type'] = stmt_type
        df_melted['as_of_date'] = pd.to_datetime(df_melted['as_of_date']).dt.date
        df_melted = df_melted.dropna(subset=['value']) # Filtrar valores nulos
        
        # Validar y convertir tipos
        df_melted['value'] = pd.to_numeric(df_melted['value'], errors='coerce')
        
        # Ingesta en PostgreSQL manejando conflictos (ON CONFLICT DO NOTHING requiere sintaxis específica en Postgres, 
        # para el MVP usamos un approach seguro de borrado e inserción del último set)
        try:
            with engine.begin() as conn:
                # Borramos los datos existentes de este ticker para evitar duplicados en la carga del MVP
                delete_query = text("DELETE FROM raw_fundamentals.yfinance_cache WHERE ticker = :t AND statement_type = :s")
                conn.execute(delete_query, {"t": ticker, "s": stmt_type})
                
            df_melted[['ticker', 'statement_type', 'metric', 'as_of_date', 'value']].to_sql(
                name='yfinance_cache',
                schema='raw_fundamentals',
                con=engine,
                if_exists='append',
                index=False
            )
            logger.info(f"Cacheados {len(df_melted)} registros de {stmt_type} para {ticker}.")
        except Exception as e:
            logger.error(f"Fallo al guardar datos para {ticker} ({stmt_type}): {e}")

def load_financials_from_cache(ticker: str, engine: Engine) -> pd.DataFrame:
    """
    Recupera los datos financieros desde la caché local en PostgreSQL.
    """
    query = f"""
        SELECT statement_type, metric, as_of_date, value 
        FROM raw_fundamentals.yfinance_cache 
        WHERE ticker = '{ticker}'
    """
    return pd.read_sql(query, engine)

def apply_pareto_filter(tickers: List[str], engine: Engine) -> pd.DataFrame:
    """
    Motor Cuantitativo: Calcula ROIC, FCF Yield y Net Debt.
    Filtra buscando Opcionalidad y Asimetría Positiva.
    POR QUÉ: Descarta el 80% del ruido corporativo, aislando negocios con 
    foso económico (moat) y riesgo de quiebra (downside) controlado.
    """
    results = []
    
    for ticker in tickers:
        try:
            # 1. Obtener datos (descarga si la caché está vacía)
            df_cache = load_financials_from_cache(ticker, engine)
            if df_cache.empty:
                logger.info(f"Caché vacía para {ticker}. Extrayendo de YFinance...")
                fetch_and_cache_financials(ticker, engine)
                df_cache = load_financials_from_cache(ticker, engine)
            
            # Pivotear temporalmente para facilitar el cálculo (usamos la fecha más reciente)
            latest_date = df_cache['as_of_date'].max()
            df_latest = df_cache[df_cache['as_of_date'] == latest_date].set_index('metric')['value']
            
            # 2. Extracción segura de métricas (Validación defensiva)
            # FCF = Operating Cash Flow - CapEx
            operating_cash_flow = df_latest.get('Operating Cash Flow', 0)
            capex = abs(df_latest.get('Capital Expenditure', 0)) # Usualmente viene negativo
            fcf = operating_cash_flow - capex
            
            # Net Debt = Total Debt - Cash
            total_debt = df_latest.get('Total Debt', 0)
            cash = df_latest.get('Cash And Cash Equivalents', 0)
            net_debt = total_debt - cash
            
            # ROIC = NOPAT / Invested Capital
            ebit = df_latest.get('Operating Income', 0)
            tax_provision = df_latest.get('Tax Provision', 0)
            pretax_income = df_latest.get('Pretax Income', 1) # Prevenir división por cero
            tax_rate = min(tax_provision / pretax_income, 0.35) if pretax_income > 0 else 0.21
            nopat = ebit * (1 - tax_rate)
            
            total_assets = df_latest.get('Total Assets', 0)
            current_liabilities = df_latest.get('Current Liabilities', 0)
            invested_capital = total_assets - current_liabilities - cash
            invested_capital = max(invested_capital, 1) # Prevenir división por cero
            
            roic = nopat / invested_capital
            
            # 3. Datos en vivo (Market Cap para EV)
            stock = yf.Ticker(ticker)
            market_cap = stock.fast_info.get('marketCap', 1)
            enterprise_value = market_cap + net_debt
            fcf_yield = fcf / enterprise_value if enterprise_value > 0 else 0
            
            # Guardar resultados
            results.append({
                'Ticker': ticker,
                'ROIC (%)': round(roic * 100, 2),
                'FCF Yield (%)': round(fcf_yield * 100, 2),
                'Net Debt': round(net_debt, 2),
                'Cash': round(cash, 2)
            })
            
        except Exception as e:
            logger.error(f"Error procesando {ticker}: {e}")
            
    df_results = pd.DataFrame(results)
    
    # 4. APLICACIÓN DE LA LEY DE POTENCIA (El Filtro)
    # Buscamos extremo derecho de la distribución (Asimetría positiva)
    if not df_results.empty:
        # Filtros matemáticos rigurosos
        mask_roic = df_results['ROIC (%)'] > 15.0#> 15.0
        mask_fcf = df_results['FCF Yield (%)'] > 1.0#> 4.0 # Superando a los bonos del tesoro
        mask_debt = df_results['Net Debt'] < 4#< 0 # Caja neta positiva (Downside blindado)
        
        oportunidades = df_results[mask_roic & mask_fcf & mask_debt]
        return oportunidades
    return pd.DataFrame()

if __name__ == "__main__":
    # Universo de prueba (Ejemplo de Tech S&P 500)
    test_tickers = [
        "AMZN",
    "IBM",
    "YPF",
    "SAN",
    "CAT",
    "ASML",
    "GGAL",
    "AAPL",
    "V",
    "UNH",
    "META",
    "MSFT",
    "AVGO",
    "LLY",
    "NVDA",
    "MU",
    "TSLA",
    "INTC",
    "GOOGL",
    "AMD",
    "JPM",
    "MELI",
    "BMA",
    "BBAR",
    "SUPV",
    "PAM",
    "CEPU",
    "TGS",
    "EDN",
    "GLOB",
    "LOMA",
    "IRS",
    "TX",
    "TS",
    "CRESY",
    "TEO",
    "CAAP"
    ]
    try:
        engine = get_db_engine()
        init_db_schema(engine)
        
        logger.info("Iniciando escaneo de asimetría fundamental...")
        mejores_oportunidades = apply_pareto_filter(test_tickers, engine)
        
        print("\n" + "="*50)
        print("🚀 RESULTADOS DEL FILTRO DE ASIMETRÍA (PARETO)")
        print("="*50)
        if not mejores_oportunidades.empty:
            print(mejores_oportunidades.to_string(index=False))
        else:
            print("Ningún activo superó los estrictos criterios de preservación y crecimiento.")
            
    except Exception as e:
        logger.critical(f"Falla crítica en el pipeline: {e}")