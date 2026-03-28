# worfflow para actualizar absolutamente todos los dashboards que necesito para monitorear mi rendimiento
1. Descargo los excels desde bull market brokers
2. Ejecuto /eargings-update {partition_date}
3. Ejecuto execute_unification.py
4. Ejecuto el pipeline para revisar que para revisar las últimas fechas dentro de historical_prices, reviso qué especies y precio faltan agrear y los agrego a partir del flujo de trabajo correspondiente, junto con el script que ingesta las cotizaciones del ccl y el mep ingest_ccl_mep.py. Si faltan cotizaciones, como por ejemplo de obligaciones o especies particulares como fideicomisos, ir a Rava bursátil o Cohen para poder obtener los precios y subirlos a mano o sinó hay un script que se llama ingest_manual_quotes.py. El script tiene que avisar exactamente la serie historica que falta de cada activo.
5. Ejecuto el script que genera la evolución del patrimonio actualmente en comparacion_portfolios_merval_s&p500.ipynb
6. Ejecuto el script que actualmente está en transferencias_para_ahorrar.ipynb
