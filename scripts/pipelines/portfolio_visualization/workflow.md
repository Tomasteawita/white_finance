# worfflow para actualizar absolutamente todos los dashboards que necesito para monitorear mi rendimiento
1. Descargo los excels desde bull market brokers
2. Ejecuto /eargings-update {partition_date}
3. Ejecuto execute_unification.py
4. Ejecute extraction_prices.py e ingest_ccl_mep.py
5. Ejecuto el script que genera la evolución del patrimonio actualmente en comparacion_portfolios_merval_s&p500.ipynb
6. Ejecuto el script que actualmente está en transferencias_para_ahorrar.ipynb
