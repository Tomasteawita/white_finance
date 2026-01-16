

## Arbol de archivos importante

white_finance/
├── config/
│   ├── assets_mapping.yaml       # Mapeo de nombres de activos (ej. AL30 -> AL30.BA)
│   └── clients_config.yaml       # Configuración de clientes (ID, Broker, Carpeta origen)
├── data/                         # [VOLUMEN DOCKER] Persistente, fuera del control de versiones
│   ├── 01_raw/                   # Archivos originales descargados
│   │   ├── cliente_A/
│   │   │   └── bull_market_mv_2025_01.xlsx
│   │   └── cliente_B/
│   │       └── balanz_mv_2025_01.xlsx
│   ├── 02_processed/             # HISTÓRICOS (Tu requisito 1)
│   │   ├── prices/
│   │   │   └── master_prices.csv # Histórico incremental de precios
│   │   └── accounts/
│   │       ├── cliente_A_cc_historica.csv
│   │       └── cliente_B_cc_historica.csv
│   └── 03_analytics/             # RESULTADOS (Tu requisito 3)
│   │   └── reports/
│   │       └── cliente_A_rendimiento_mensual.csv
├── notebooks/                    # ORQUESTADORES
│   ├── 01_ingest_broker_data.ipynb  # Procesa novedades de cuentas corrientes
│   ├── 02_update_market_data.ipynb  # Actualiza precios históricos
│   └── 03_generate_reports.ipynb    # Genera Foto y Película (Cálculo VCP)
├── src/                          # CÓDIGO FUENTE (Lógica Pura)
│   ├── __init__.py
│   ├── connectors/               # STRATEGY PATTERN
│   │   ├── __init__.py
│   │   ├── base_strategy.py      # Clase abstracta
│   │   ├── bull_market.py        # Implementación Bull
│   │   └── balanz.py             # Implementación Balanz
│   ├── core/
│   │   ├── account_manager.py    # Lógica de merge/upsert de cuentas corrientes
│   │   ├── pricing_engine.py     # Lógica para bajar/guardar precios
│   │   └── fci_calculator.py     # Lógica de Cuotapartes (Rendimiento)
│   └── utils/
│       └── file_io.py
├── Dockerfile
├── docker-compose.yml
└── requirements.txt