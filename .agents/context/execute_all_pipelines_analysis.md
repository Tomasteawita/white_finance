# Análisis Completo: `execute_all_pipelines.py`

> [!IMPORTANT]
> Este documento analiza qué ocurre al ejecutar [execute_all_pipelines.py](file:///c:/Users/tomas/white_finance/scripts/pipelines/execute_all_pipelines.py) con el venv del repositorio `C:\Users\tomas\white_finance\`.

---

## 1. Visión General del Script

El script orquesta **6 pipelines secuenciales** que constituyen el flujo completo de actualización de datos financieros personales. Cada pipeline depende del anterior.

```mermaid
flowchart TD
    A["1. refresh_earnings()"] --> B["2. ingest_ccl_mep()"]
    B --> C["3. execute_unification()"]
    C --> D["4. extraction_pipeline.run_from_cuentas_corrientes()"]
    D --> E["5. evolucion_historica_patrimonio.run()"]
    E --> F["6. execution_transferencias_para_ahorrar.execute()"]

    A -- "AWS S3 + Step Functions + PowerShell" --> A1["Excel → CSV → S3 → Lambda → Histórico"]
    B -- "yfinance + PostgreSQL" --> B1["Cotización GGAL → CCL/MEP → DB"]
    C -- "CSV local" --> C1["3 CSVs → 1 CSV unificado"]
    D -- "yfinance + IOL API + PostgreSQL" --> D1["Especies faltantes → historical_prices"]
    E -- "PostgreSQL + CSV" --> E1["Holdings + Precios → Evolución patrimonio"]
    F -- "CSV local" --> F1["Filtro transferencias → CSV"]
```

---

## 2. Inicialización del Script

```python
pipeline = ExecuteAllPipelines(partition_date="hoy")
pipeline.execute_all_pipelines()
```

Al instanciarse `ExecuteAllPipelines`:
- Se crea `ExtractionPipeline()` → **conecta a PostgreSQL** inmediatamente (`_init_db`)
- Se crea `EvolucionHistoricaPatrimonio()` → **conecta a PostgreSQL** y descarga `ratios_cedears`
- Se crea `ExecutionTransferenciasParaAhorrar()` → solo define paths relativos

> [!WARNING]
> En la inicialización ya se realizan **2 conexiones a PostgreSQL** y **1 query** a `earnings.ratios_cedears`. Si la DB está apagada, el script falla antes de ejecutar cualquier pipeline.

---

## 3. Pipeline 1: `refresh_earnings()`

**Archivo**: [refresh_earnings.py](file:///c:/Users/tomas/white_finance/scripts/pipelines/AWS/refresh_earnings.py)

### ¿Qué hace?
Procesa archivos Excel de cuentas corrientes de Bull Market descargados manualmente, los sube a AWS S3 y dispara una Step Function para generar el histórico consolidado.

### Flujo detallado

| Paso | Acción | Detalle |
|------|--------|---------|
| 1 | **Solicitar fecha** | Pide fecha por `input()` interactivo (no recibe `--fecha` porque `execute_all_pipelines` llama `main()` sin args) |
| 2 | **Copiar Excels** | Busca en `C:\Users\tomas\Downloads\` archivos `Cuenta Corriente {PESOS/DOLARES/DOLARES CABLE} {dd-MM-yy}.xlsx` → copia a `C:\Users\tomas\withe-finance-ingest\` |
| 3 | **Ejecutar PS1** | Llama a [ingest_cuenta_corriente_auto.ps1](file:///c:/Users/tomas/white_finance/scripts/layers/AWS/raw/ingest/ingest_cuenta_corriente_auto.ps1) para cada moneda (PESOS, DOLARES, DOLARES CABLE) |
| 4 | **Notificación** | Imprime que debe actualizarse `notebooks/ganancias_realizadas.ipynb` |

### El script PowerShell (PS1) hace:

```mermaid
flowchart LR
    A["Excel local"] --> B["Python validator\n(main.py)"]
    B --> C["CSV local"]
    C --> D["aws s3 sync\n→ withefinance-raw"]
    D --> E["AWS Step Function\nWitheFinance-Historical-Profits"]
    E --> F["Lambda: S3Ingest-Raw"]
    F --> G["Lambda: GenCuentaCorrienteHistorico"]
    G --> H["Lambda: GenHistoricalProfit"]
    H --> I["Download\nhistórico actualizado\n→ data/analytics/"]
```

### Servicios externos tocados
| Servicio | Acción |
|----------|--------|
| **AWS S3** (`withefinance-raw`) | Sube CSVs de cuentas corrientes |
| **AWS Step Functions** | Dispara máquina de estados `WitheFinance-Historical-Profits` |
| **3 AWS Lambdas** | Ingest → Consolidación histórica → Profit histórico |
| **AWS SES** | Envía emails de alerta/éxito a `tcueva.cloud@gmail.com` |
| **AWS S3** (`withefinance-integrated`, `whitefinance-analytics`) | Descarga archivos históricos actualizados |

### Archivos leídos (INPUT)
| Path | Descripción |
|------|-------------|
| `C:\Users\tomas\Downloads\Cuenta Corriente PESOS {dd-MM-yy}.xlsx` | Excel de cuenta corriente en pesos |
| `C:\Users\tomas\Downloads\Cuenta Corriente DOLARES {dd-MM-yy}.xlsx` | Excel de cuenta corriente en USD MEP |
| `C:\Users\tomas\Downloads\Cuenta Corriente DOLARES CABLE {dd-MM-yy}.xlsx` | Excel de cuenta corriente en USD Cable |

### Archivos escritos (OUTPUT)
| Path | Descripción |
|------|-------------|
| `C:\Users\tomas\withe-finance-ingest\*.xlsx` | Copia de los Excels originales |
| `C:\Users\tomas\withe-finance-ingest\cuenta_corriente-{YYYYMMDD}.csv` | CSV validado (PESOS) |
| `C:\Users\tomas\withe-finance-ingest\cuenta_corriente_dolares-{YYYYMMDD}.csv` | CSV validado (DOLARES) |
| `C:\Users\tomas\withe-finance-ingest\cuenta_corriente_dolares_cable-{YYYYMMDD}.csv` | CSV validado (DOLARES CABLE) |
| `data\analytics\cuenta_corriente_historico.csv` | Descargado desde S3 post Step Function |
| `data\analytics\cuenta_corriente_dolares_historico.csv` | Descargado desde S3 post Step Function |
| `data\analytics\cuenta_corriente_dolares_cable_historico.csv` | Descargado desde S3 post Step Function |
| `data\analytics\profit.csv` | Descargado desde S3 post Step Function |

> [!CAUTION]
> **Punto de bloqueo**: `solicitar_fecha()` usa `input()` interactivo. Si se ejecuta sin `--fecha`, el script **se bloquea esperando entrada del usuario por consola**. Además, `argparse` procesa `sys.argv`, que en el contexto de `execute_all_pipelines.py` puede capturar argumentos no deseados.

> [!WARNING]
> **Prerequisitos manuales**: Los Excels deben estar previamente descargados en `Downloads`. Si no existen, el paso 2 emite warnings pero continúa. El PS1 falla con `exit 1` si no encuentra el Excel.

---

## 4. Pipeline 2: `ingest_ccl_mep()`

**Archivo**: [ingest_ccl_mep.py](file:///c:/Users/tomas/white_finance/scripts/layers/portfolio_visualization/ingest_ccl_mep.py)

### ¿Qué hace?
Calcula los tipos de cambio **CCL (Contado con Liquidación)** y **MEP (Mercado Electrónico de Pagos)** usando cotizaciones de GGAL (Grupo Financiero Galicia) y los inserta en PostgreSQL.

### Fórmulas de cálculo

```
CCL = (GGAL.BA × 10) / GGAL (ADR NYSE)
MEP = GGAL.BA / GGALD.BA
```

### Flujo

| Paso | Acción |
|------|--------|
| 1 | Conecta a PostgreSQL local (`postgres:postgres@localhost:5432/postgres`) |
| 2 | Crea tabla `earnings.ccl_mep` si no existe (`date DATE PK, ccl NUMERIC, mep NUMERIC`) |
| 3 | Descarga desde Yahoo Finance: `GGAL`, `GGAL.BA`, `GGALD.BA` (desde 2020-01-01 hasta hoy) |
| 4 | Calcula CCL y MEP con las fórmulas |
| 5 | Inserta/actualiza fila por fila con `ON CONFLICT DO UPDATE` |

### Servicios externos
| Servicio | Acción |
|----------|--------|
| **Yahoo Finance** (via `yfinance`) | Descarga cotizaciones de GGAL, GGAL.BA, GGALD.BA |
| **PostgreSQL local** | Escribe en `earnings.ccl_mep` |

### Datos escritos
| Tabla | Esquema |
|-------|---------|
| `earnings.ccl_mep` | `date DATE PK, ccl NUMERIC, mep NUMERIC` |

> [!NOTE]
> Este pipeline es **idempotente**: usa `ON CONFLICT DO UPDATE`, por lo que ejecutarlo múltiples veces es seguro. Descarga todo el historial desde 2020 cada vez (~1400+ filas).

---

## 5. Pipeline 3: `execute_unification()`

**Archivo**: [execute_unification.py](file:///c:/Users/tomas/white_finance/scripts/pipelines/portfolio_visualization/execute_unification.py)
**Layer**: [unify_accounts_db.py](file:///c:/Users/tomas/white_finance/scripts/layers/portfolio_visualization/unify_accounts_db.py)

### ¿Qué hace?
Unifica las 3 cuentas corrientes (Pesos, MEP, Cable) en un único dataset ordenado cronológicamente, etiquetando cada registro con su moneda de origen.

### Flujo

| Paso | Acción |
|------|--------|
| 1 | Lee 3 CSVs de cuentas corrientes históricas |
| 2 | Agrega columna `Origen` (`ARS`, `USD MEP`, `USD CCL`) a cada uno |
| 3 | Concatena los 3 DataFrames |
| 4 | Ordena por `Liquida`, `Operado`, `Numero` |
| 5 | Guarda CSV unificado |

### Archivos leídos (INPUT)
| Path | Tamaño actual |
|------|---------------|
| `data\analytics\cuenta_corriente_historico.csv` | ~61 KB |
| `data\analytics\cuenta_corriente_dolares_historico.csv` | ~5.6 KB |
| `data\analytics\cuenta_corriente_dolares_cable_historico.csv` | ~9.4 KB |

### Archivos escritos (OUTPUT)
| Path | Tamaño actual |
|------|---------------|
| `data\analytics\cuentas_unificadas_sorted.csv` | ~81 KB |

### Columnas del output
```
Liquida, Operado, Comprobante, Numero, Cantidad, Especie, Precio, Importe, Saldo, Referencia, Origen
```

> [!NOTE]
> Pipeline **100% local**, sin conexiones externas. Rápido y seguro. Es un prerequisito crítico para los pipelines 4, 5 y 6.

---

## 6. Pipeline 4: `extraction_pipeline.run_from_cuentas_corrientes()`

**Archivo**: [extraction_prices.py](file:///c:/Users/tomas/white_finance/scripts/pipelines/portfolio_visualization/extraction_prices.py)
**Extractores**: [yfinance_extractor.py](file:///c:/Users/tomas/white_finance/scripts/layers/portfolio_visualization/extractors/yfinance_extractor.py), [iol_extractor.py](file:///c:/Users/tomas/white_finance/scripts/layers/portfolio_visualization/extractors/iol_extractor.py)

### ¿Qué hace?
Lee las especies (tickers) del CSV unificado, detecta cuáles faltan o están desactualizadas en la base de datos, y descarga sus cotizaciones históricas usando una cadena de fallback: **YFinance → IOL (InvertirOnline)**.

### Flujo detallado

```mermaid
flowchart TD
    A["Lee cuentas_unificadas_sorted.csv\n(columna Especie)"] --> B["Clasifica por mercado:\n.US → nYSE/nASDAQ\nresto → bCBA"]
    B --> C["Consulta earnings.ratios_cedears\n(última partición)"]
    C --> D["Left join: si especie es CEDEAR\n→ mercado = nYSE/nASDAQ"]
    D --> E["Consulta max_date en\nearnings.historical_prices"]
    E --> F{"¿Tiene datos?"}
    F -- "No" --> G["Extraer desde 2023-01-01"]
    F -- "Sí" --> H["Extraer desde 2023-01-01\n(forzado completo)"]
    G --> I["Cadena: YFinance → IOL"]
    H --> I
    I --> J["_save_to_db:\nDELETE + INSERT en\nearnings.historical_prices"]
```

### Arquitectura de Extractores (Template Method Pattern)

| Extractor | Autenticación | Fuente | Source tag |
|-----------|---------------|--------|------------|
| [YFinanceExtractor](file:///c:/Users/tomas/white_finance/scripts/layers/portfolio_visualization/extractors/yfinance_extractor.py) | No requiere | Yahoo Finance | `YFinance` (bCBA) / `YFinance_USD` (NYSE) |
| [IOLExtractor](file:///c:/Users/tomas/white_finance/scripts/layers/portfolio_visualization/extractors/iol_extractor.py) | OAuth2 Bearer Token | InvertirOnline API | `API_IOL_{mercado}` |

### Tabla de base de datos impactada
```sql
earnings.historical_prices (
    ticker VARCHAR,
    date DATE,
    open NUMERIC,
    high NUMERIC,
    low NUMERIC,
    close NUMERIC,
    volume NUMERIC,
    source VARCHAR,
    -- PK: (ticker, date, source)
)
```

### Servicios externos
| Servicio | Acción |
|----------|--------|
| **Yahoo Finance** | Descarga OHLCV por especie |
| **InvertirOnline API** (`api.invertironline.com`) | OAuth2 auth + descarga series históricas |
| **PostgreSQL local** | Lee `ratios_cedears`, `historical_prices`; escribe en `historical_prices` |

### Credenciales utilizadas
| Variable `.env` | Servicio |
|-----------------|----------|
| `POSTGRE_USER`, `POSTGRE_PASSWORD`, `POSTGRE_HOST`, `POSTGRE_PORT`, `POSTGRE_DB` | PostgreSQL |
| `USERNAME_IOL`, `PASSWORD_IOL` | InvertirOnline API |

### Token cache
El [IOLManager](file:///c:/Users/tomas/white_finance/scripts/layers/portfolio_visualization/extractors/iol_manager.py) persiste tokens en [.iol_token_cache.json](file:///c:/Users/tomas/white_finance/scripts/layers/portfolio_visualization/extractors/.iol_token_cache.json).

> [!WARNING]
> **Pipeline más lento y riesgoso**: Itera especie por especie, hace DELETE + INSERT por cada una. Si hay muchas especies (~30-50), puede tardar **varios minutos**. Cada fallo de YFinance delega a IOL, duplicando latencia. La operación DELETE-antes-de-INSERT puede causar **pérdida temporal de datos** si el INSERT falla después del DELETE.

> [!NOTE]
> `PrimaryExtractor` está comentado en la lista de extractores (línea 36), así que solo opera YFinance → IOL.

---

## 7. Pipeline 5: `evolucion_historica_patrimonio.run()`

**Archivo**: [execute_evolucion_patrimonio.py](file:///c:/Users/tomas/white_finance/scripts/pipelines/portfolio_visualization/execute_evolucion_patrimonio.py)

### ¿Qué hace?
Genera la **evolución histórica del patrimonio en USD**, reconstruyendo día a día las posiciones del portafolio, valorizándolas con precios de mercado, y comparándolas contra benchmarks (SPY, ARGT).

### Flujo detallado

| Paso | Acción |
|------|--------|
| 1 | Lee `cuentas_unificadas_sorted.csv` |
| 2 | `process_transactions()`: Recorre todas las transacciones cronológicamente, actualiza saldos de cash (ARS, MEP, CCL) y cantidades nominales de cada activo |
| 3 | Genera holdings diarios con forward-fill hasta hoy |
| 4 | `get_market_data()`: Descarga toda `earnings.historical_prices` + `earnings.ccl_mep` de PostgreSQL |
| 5 | Pivotea precios por ticker, reindexea a las fechas de holdings |
| 6 | Multiplica nominales × precio USD por activo para obtener valuación |
| 7 | Convierte Cash ARS y FCIs a USD usando CCL |
| 8 | Clasifica activos en **Safe** vs **Growth** (estrategia Barbell) |
| 9 | `gen_benchmarks()`: Simula qué hubiera pasado si todo el capital se hubiera invertido en SPY o ARGT |
| 10 | Escribe CSV final con evolución + benchmarks |

### Lógica de negocio clave

**Clasificación Barbell:**
| Tipo | Activos |
|------|---------|
| **Safe** | SNSBO, GD35, GD30, AL30, AE38, LK01Q, BMACTAA |
| **Cash** | Cash_ARS, Cash_MEP, Cash_CCL |
| **Growth** | Todo lo demás (acciones, CEDEARs, etc.) |

**Transformaciones especiales:**
- **CEDEARs**: Cantidades ajustadas por ratio de conversión (ej: AAPL ratio 10, GOOGL ratio 58)
- **Bonos** (SNSBO, GD35, etc.): Precio dividido por 100 (cotización en 100 nominales)
- **FCIs abiertos** (ALGIIIA, BMACTAA, BULL-IA, BULMAAA, RIGAHOR): Se trackean por importe, no por nominales

### Archivos leídos
| Fuente | Dato |
|--------|------|
| `data\analytics\cuentas_unificadas_sorted.csv` | Transacciones históricas |
| PostgreSQL `earnings.historical_prices` | Precios OHLCV de todas las especies |
| PostgreSQL `earnings.ccl_mep` | Tipos de cambio CCL/MEP diarios |

### Archivos escritos
| Path | Descripción |
|------|-------------|
| `data\analytics\portfolio_visualization_data\evolucion_patrimonio.csv` | **Output final** (~132 KB) con columnas: `Fecha, Cash_Total_USD, Total_Safe_Valuation, Total_Growth_Valuation, Patrimonio_USD, patrimonio_spy, patrimonio_argt` |

### Servicios externos
| Servicio | Acción |
|----------|--------|
| **PostgreSQL local** | Lee `historical_prices`, `ccl_mep`, `ratios_cedears` |

> [!WARNING]
> Este pipeline hace `.iterrows()` sobre TODAS las transacciones históricas + TODOS los días de benchmark. Es computacionalmente intensivo pero no hace llamadas a APIs externas.

---

## 8. Pipeline 6: `execution_transferencias_para_ahorrar.execute()`

**Archivo**: [execute_transferencias_para_ahorrar.py](file:///c:/Users/tomas/white_finance/scripts/pipelines/portfolio_visualization/execute_transferencias_para_ahorrar.py)

### ¿Qué hace?
Filtra las transferencias (ingresos/egresos de dinero) del historial de cuenta corriente en pesos para generar un reporte de ahorro.

### Flujo

| Paso | Acción |
|------|--------|
| 1 | Lee `cuenta_corriente_historico.csv` |
| 2 | Filtra columnas: `Liquida`, `Comprobante`, `Importe` |
| 3 | Filtra filas donde `Comprobante` sea `RECIBO DE COBRO` u `ORDEN DE PAGO` |
| 4 | Exporta a CSV |

### Archivos escritos
| Path | Descripción |
|------|-------------|
| `data\analytics\Transferencias Bull Market.csv` | ~5.2 KB actualmente |

> [!CAUTION]
> **Bug de paths relativos**: Usa `../../data/analytics/` como path relativo. Cuando se ejecuta desde `execute_all_pipelines.py` (que está en `scripts/pipelines/`), el CWD puede no ser `scripts/pipelines/portfolio_visualization/`, causando que **escriba el archivo en un directorio incorrecto** o falle por no encontrar el archivo de entrada.

---

## 9. Resumen de Dependencias Externas

### Servicios y Conectividad

| Servicio | Pipeline(s) | Requerido |
|----------|-------------|-----------|
| **PostgreSQL** (`localhost:5432`) | 2, 4, 5 | ✅ Crítico |
| **Yahoo Finance** (internet) | 2, 4 | ✅ Crítico |
| **InvertirOnline API** (internet) | 4 | ⚠️ Fallback |
| **AWS CLI** (configurado) | 1 | ✅ Para pipeline 1 |
| **AWS S3** | 1 | ✅ Para pipeline 1 |
| **AWS Step Functions** | 1 | ✅ Para pipeline 1 |
| **AWS Lambda** | 1 | ✅ Para pipeline 1 |
| **PowerShell** | 1 | ✅ Para pipeline 1 |

### Archivos de Datos (Estado Actual)

| Archivo | Existe | Tamaño |
|---------|--------|--------|
| `data\analytics\cuenta_corriente_historico.csv` | ✅ | 61 KB |
| `data\analytics\cuenta_corriente_dolares_historico.csv` | ✅ | 5.6 KB |
| `data\analytics\cuenta_corriente_dolares_cable_historico.csv` | ✅ | 9.4 KB |
| `data\analytics\cuentas_unificadas_sorted.csv` | ✅ | 81 KB |
| `data\analytics\portfolio_visualization_data\evolucion_patrimonio.csv` | ✅ | 132 KB |
| `data\analytics\Transferencias Bull Market.csv` | ✅ | 5.2 KB |
| `data\analytics\profit.csv` | ✅ | 8.8 KB |

### Variables de Entorno Necesarias (de [.env](file:///c:/Users/tomas/white_finance/.env))

| Variable | Usado por |
|----------|-----------|
| `POSTGRE_USER` | Pipelines 2, 4 |
| `POSTGRE_PASSWORD` | Pipelines 2, 4 |
| `POSTGRE_HOST` | Pipelines 2, 4 |
| `POSTGRE_PORT` | Pipelines 2, 4 |
| `POSTGRE_DB` | Pipelines 2, 4 |
| `USERNAME_IOL` | Pipeline 4 (IOL fallback) |
| `PASSWORD_IOL` | Pipeline 4 (IOL fallback) |

### Tablas de PostgreSQL involucradas

| Tabla | Operación | Pipeline(s) |
|-------|-----------|-------------|
| `earnings.ccl_mep` | CREATE IF NOT EXISTS + UPSERT / READ | 2, 5 |
| `earnings.historical_prices` | DELETE + INSERT / READ | 4, 5 |
| `earnings.ratios_cedears` | READ | 4, 5 |

---

## 10. ¿Qué Pasa Exactamente al Ejecutarlo?

### Escenario A: Sin preparación previa (ejecución "en frío")

1. **Se bloquea en Pipeline 1** esperando `input()` para la fecha
2. Si se ingresa una fecha, busca Excels en Downloads que probablemente no existen
3. Los 3 scripts PS1 fallan (`exit 1`) porque no encuentran Excels
4. Pipeline 1 termina con warnings pero **no lanza excepción** → continúa
5. **Pipeline 2** funciona si PostgreSQL está corriendo y hay internet → escribe CCL/MEP en DB
6. **Pipeline 3** funciona si los CSVs de cuentas corrientes existen (están presentes actualmente)
7. **Pipeline 4** funciona si PostgreSQL está corriendo y hay internet → descarga precios
8. **Pipeline 5** funciona si Pipeline 3 y 4 completaron bien → genera evolución patrimonio
9. **Pipeline 6** probablemente **falla o escribe en ruta incorrecta** por paths relativos

### Escenario B: Con PostgreSQL apagado

1. **Falla en la inicialización** de `ExtractionPipeline()` o `EvolucionHistoricaPatrimonio()` al intentar conectar a PostgreSQL y consultar `ratios_cedears`.
2. **El script nunca llega a ejecutar ningún pipeline**.

### Escenario C: Happy path (todo preparado)

1. Excels descargados → PS1 ejecuta → S3 + Step Functions procesan → CSVs actualizados
2. YFinance descarga GGAL → CCL/MEP actualizado en DB
3. 3 CSVs unificados → `cuentas_unificadas_sorted.csv`
4. Especies extraídas de CSV → precios descargados vía YFinance/IOL → DB actualizada
5. Holdings recalculados + benchmarks SPY/ARGT → CSV de evolución patrimonial
6. Transferencias filtradas → CSV de ahorro

**Tiempo estimado (happy path): 5-15 minutos** dependiendo de la cantidad de especies y la velocidad de Yahoo Finance/IOL.

---

## 11. Bugs y Riesgos Identificados

### 🔴 Críticos

| # | Problema | Archivo | Línea(s) |
|---|----------|---------|----------|
| 1 | **`input()` bloquea ejecución** sin GUI ni argumento `--fecha` | [refresh_earnings.py](file:///c:/Users/tomas/white_finance/scripts/pipelines/AWS/refresh_earnings.py#L57) | 57 |
| 2 | **`argparse` puede capturar argumentos** del script padre | [refresh_earnings.py](file:///c:/Users/tomas/white_finance/scripts/pipelines/AWS/refresh_earnings.py#L186-L194) | 186-194 |
| 3 | **Paths relativos en Pipeline 6** (`../../data/analytics/`) dependen del CWD | [execute_transferencias_para_ahorrar.py](file:///c:/Users/tomas/white_finance/scripts/pipelines/portfolio_visualization/execute_transferencias_para_ahorrar.py#L7-L21) | 7, 21 |

### 🟡 Moderados

| # | Problema | Archivo |
|---|----------|---------|
| 4 | **DELETE antes de INSERT** en historical_prices puede causar ventana de datos vacíos si falla el INSERT | [base_extractor.py](file:///c:/Users/tomas/white_finance/scripts/layers/portfolio_visualization/extractors/base_extractor.py#L116-L122) |
| 5 | **Hardcoded DB URI** `postgresql://postgres:postgres@localhost:5432/postgres` en `EvolucionHistoricaPatrimonio` ignora `.env` | [execute_evolucion_patrimonio.py](file:///c:/Users/tomas/white_finance/scripts/pipelines/portfolio_visualization/execute_evolucion_patrimonio.py#L20) |
| 6 | **Ratios de CEDEAR hardcodeados** en `UnifiedAccountPricer` vs los de DB — posible inconsistencia | [unify_accounts_db.py](file:///c:/Users/tomas/white_finance/scripts/layers/portfolio_visualization/unify_accounts_db.py#L24-L33) |
| 7 | **Sin manejo de excepciones** entre pipelines — si Pipeline 3 falla, Pipeline 4 intenta correr con datos viejos | [execute_all_pipelines.py](file:///c:/Users/tomas/white_finance/scripts/pipelines/execute_all_pipelines.py#L26-L43) |

### 🟢 Menores

| # | Problema |
|---|----------|
| 8 | `partition_date="hoy"` se pasa pero nunca se usa realmente |
| 9 | Pipeline 2 descarga todo el historial desde 2020 cada vez (ineficiente pero funcional) |
| 10 | `BULL-IA` está en `fcis_abiertos` pero no se procesa como FCI en el loop de conversión a USD (línea 308) |

---

## 12. Dependencias Python (requirements.txt)

| Paquete | Versión | Usado por |
|---------|---------|-----------|
| `pandas` | any | Todos los pipelines |
| `yfinance` | any | Pipelines 2, 4 |
| `sqlalchemy` | ≥2.0.35 | Pipelines 2, 4, 5 |
| `psycopg2` | any | Driver PostgreSQL |
| `python-dotenv` | 1.0.1 | Lectura de `.env` |
| `openpyxl` | 3.1.5 | Lectura de Excels |
| `requests` | 2.32.3 | IOL API |
| `numpy` | (via pandas) | Pipeline 5 |
| `lxml` | 5.3.0 | Parsing XML |
| `s3fs` | 2024.9.0 | Acceso S3 |
| `polars` | 0.20.31 | No usado directamente aquí |

---

## 13. Diagrama de Arquitectura Completo

```mermaid
graph TB
    subgraph "execute_all_pipelines.py"
        P1["Pipeline 1\nrefresh_earnings"]
        P2["Pipeline 2\ningest_ccl_mep"]
        P3["Pipeline 3\nexecute_unification"]
        P4["Pipeline 4\nextraction_prices"]
        P5["Pipeline 5\nevolucion_patrimonio"]
        P6["Pipeline 6\ntransferencias"]
    end

    subgraph "Datos Locales"
        DL1["Downloads/*.xlsx"]
        DL2["withe-finance-ingest/"]
        DA1["data/analytics/cc_historico*.csv"]
        DA2["data/analytics/cuentas_unificadas_sorted.csv"]
        DA3["data/analytics/pvd/evolucion_patrimonio.csv"]
        DA4["data/analytics/Transferencias Bull Market.csv"]
    end

    subgraph "PostgreSQL (localhost:5432)"
        DB1["earnings.ccl_mep"]
        DB2["earnings.historical_prices"]
        DB3["earnings.ratios_cedears"]
    end

    subgraph "APIs Externas"
        YF["Yahoo Finance"]
        IOL["InvertirOnline API"]
    end

    subgraph "AWS (us-east-2)"
        S3R["S3: withefinance-raw"]
        S3I["S3: withefinance-integrated"]
        S3A["S3: whitefinance-analytics"]
        SF["Step Function"]
        L1["Lambda: S3Ingest"]
        L2["Lambda: GenHistorico"]
        L3["Lambda: GenProfit"]
        SES["SES: Email alerts"]
    end

    DL1 --> P1
    P1 --> DL2
    P1 --> S3R
    S3R --> SF
    SF --> L1 --> L2 --> L3
    L3 --> S3I
    L3 --> S3A
    S3I --> DA1
    SF --> SES

    YF --> P2
    P2 --> DB1

    DA1 --> P3
    P3 --> DA2

    DA2 --> P4
    DB3 --> P4
    YF --> P4
    IOL --> P4
    P4 --> DB2

    DA2 --> P5
    DB1 --> P5
    DB2 --> P5
    DB3 --> P5
    P5 --> DA3

    DA1 --> P6
    P6 --> DA4
```

---

## 14. Recomendaciones para Futuras Tareas

1. **Para ejecutar localmente**: Asegurar PostgreSQL corriendo con schema `earnings` y las tablas necesarias creadas.
2. **Para saltear Pipeline 1**: Si los CSVs de cuentas corrientes ya están actualizados, se puede empezar directamente desde Pipeline 2.
3. **Para debug**: Ejecutar cada pipeline individualmente en lugar del orquestador monolítico.
4. **Para refactorizar**: El bug más impactante es el `input()` de Pipeline 1 y los paths relativos de Pipeline 6.
