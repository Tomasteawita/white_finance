Estoy intentando generar un script que genere un CSV, el cual tenga 4 columnas:

- Operado: Fecha en la que se realizo la operacion.

- Cash_Total_USD: Importe liquido.

- Total_Safe_Valuation: Valor total en dolares de los activos de renta fija (Bonos Soberanos, Obligaciones Neociables, Cauciones)

- Total_Growth_Valuation: Valor total en dolares de los activos de renta variable (Acciones Argentinas, Acciones del exterior, ETFs del exterior, Fondos Comunes de Inversion Abiertos, CeDeArs)

- Patrimonio_USD: La suma entre Cash_Total_USD + Total_Safe_Valuation + Total_Growth_Valuation



Las columnas son necesarias para ver cuanto creción mi patrimonio en dolares a lo largo del tiempo con un grafico de lineas, a su vez, me sirve para ver cuanto de mi patrimonio está distribuido entre activos de riesgo y conservadores.

Teniendo en cuenta el objetivo antes mencionado y el siguiente prompt que explica la lógica de negocio que hay que seguir para entender como funciona el movimiento entre las diferentes cuentas corrientes que tienen el movimiento de mi patrimonio en la bolsa de valores:

```markdown

# Visualización de patrimonio en bolsa

## Contexto

Gestionamos el patrimonio de un cliente dentro de un broker de bolsa regulado por la comisión nacional de valores en Argentina. El cliente tiene tres tipos de cuentas:

- Cuenta en pesos

- Cuenta en dolares mep

- Cuanta en dolares cable (CCL)

El cliente puede tener operaciones que sean con cuentas cruzadas, eso quiere decir que una operacion puede tener una cuenta de origen en la que compra el activo y una cuenta destino en la que se puede vender el mismo activo pero con la denominación en otra moneda o cobrar intereses o amortizaciones en otra moneda.

El cliente compra activos en pesos y los vende en dolares mep o ccl, o compra activos en dolares mep o ccl y los vende en pesos.

El abanico de activos es:

- Acciones Argentinas

- Bonos Soberanos

- Obligaciones Negociables

- CeDeArs

- Fondo Comunes de Inversión Abiertos

- Cauciones

- Acciones del exterior (cotizan en el NYSE)

- ETFs del exterior



## Objetivo

Obtener un archivo csv que tenga la siguiente estructura:

- Operado: Fecha en la que se realizo la operacion.

- Cash_Total_USD: Importe liquido.

- Total_Safe_Valuation: Valor total en dolares de los activos de renta fija (Bonos Soberanos, Obligaciones Neociables, Cauciones)

- Total_Growth_Valuation: Valor total en dolares de los activos de renta variable (Acciones Argentinas, Acciones del exterior, ETFs del exterior, Fondos Comunes de Inversion Abiertos, CeDeArs)

- Patrimonio_USD: La suma entre Cash_Total_USD + Total_Safe_Valuation + Total_Growth_Valuation

A partir de un único csv llamado cuentas_unificadas_sorted.csv, el cual se puede manipular libremente.



## Lógica de negocio de la cuenta corriente unificada ordenada

cuentas_unificadas_sorted.csv tiene las 3 cuentas corrientes (pesos, mep, ccl). Ordenadas por las columnas Liquida y Operado. Tienen una columna llamada "Origen":

- ARS marca la cuenta corriente en pesos

- USD MEP marca la cuenta corriente en dolares mep

- USD CCL marca la cuenta corriente en dolares mep



### Analisis registro por registro

#### Tipos de activos

* Siempre que una especie este dentro de este diccionario y el origen sea ARS, quiere decir que son Cedears:

```python

ratios_cedear = {

            'KO': 5.0, 'SPY': 20.0, 'QQQ': 20.0, 'AAPL': 10.0,

            'GOOGL': 58.0, 'MSFT': 30.0, 'TSLA': 15.0, 'MELI': 120.0,

            'LLY': 56.0, 'META': 24.0, 'VIST': 3.0, 'AMZN': 144.0,

            'NVDA': 24.0, 'NFLX': 60.0, 'TLT': 1.0, 'SH': 8.0,

            'ARGT': 1.0, 'XLP': 1.0, 'SHY': 1.0, 'ADBE': 44.0,

            'ARKK': 10.0, 'ASML': 146.0, 'BBD': 1.0, 'BIOX': 1.0,

            'COIN': 27.0, 'ERIC': 2.0, 'HMY': 1.0, 'LAR': 1.0,

            'PAAS': 3.0, 'PSQ': 8.0, 'SAN': 0.25, 'UNH': 33.0, 'VALE': 2.0

        }

```

* Siempre que una especie termine con el sufijo ".US" y su origen sea USD CCL, es una especie comprada en dolares cable en el exterior.

* Siempre que una especie esté dentro de ['SNSBO', 'GD350','GD30','AL30','AE38'] quiere decir que es un bono, una nota o un fideicomiso financiero, para saber cual es su precio verdadero, hay que dividir su precio por 100, dado que los precios están expresados por 100 nominales del activo.

* Una caución es un instrumento financiero que se utiliza para obtener rendimientos a corto plazo con saldos liquidos.

* Los fondos comunes de inversión abiertos en las cuentas corrientes no tienen un precio, solo un importe al momento de ser subscriptos



#### Ingresos y egresos

Los registros con estos atributos:

- Comprobante RECIBO DE COBRO

- Referencia CREDITO CTA. CTE.

- Origen ARS

Indican un aumento en el cash liquido de la cuenta corriente en pesos, como a su vez un aumento en el patrimonio en pesos.

---

Los registros con estos atributos:

- Comprobante REC COBRO DOLARES

- Referencia MEP CREDITO CTA. CTE.

- Origen USD MEP

Indican un aumento en el cash liquido de la cuenta corriente en dolares mep, como a su vez un aumento en el patrimonio en dolares mep.

---

Los registros con estos atributos:

- Comprobante ORDEN DE PAGO

- Referencia TRANSFERENCIA VIA MEP

- Origen ARS

Indican una disminucion en el cash liquido de la cuenta corriente en pesos, como a su vez una disminucion en el patrimonio en pesos.

---

Los registros con estos atributos:

- Comprobante ORD PAGO DOLARES

- Referencia TRANSFERENCIA VIA MEP

- Origen USD MEP

Indican una disminucion en el cash liquido de la cuenta corriente en dolares mep, como a su vez una disminucion en el patrimonio en dolares mep.



#### Compra y venta de monedas (compra/venta dolares mep y cable)

Para comprar dolares mep y cable se sigue la siguiente lógica:

1. Se compra un activo denominado en una moneda en pesos o en dolares

2. Se vende el mismo activo en la moneda deseada que puede ser mep o cable

* Ejemplo de compra de dolares mep en la cuenta unificada:

```csv

2024-10-23,2024-10-23,COMPRA NORMAL,9276340,326.0,AL30,0.6138461323719334,-200.11383913459028,139.40570292432128,,ARS

2024-10-25,2024-10-24,VENTA PARIDAD,9315960,-326.0,AL30,0.6173899296931468,201.26925332619834,201.74315326535546,,USD MEP

```

* Ejemplo de compra de dolares CCL en la cuenta unificada:

```csv

2024-11-29,2024-11-29,COMPRA NORMAL,10454660,11.0,KO,64.60807786068473,-710.6888564810653,91.47119837544973,,ARS

2024-12-02,2024-11-29,VENTA EXTERIOR V,32715,-11.0,KO,63.722727,700.95,701.71,,USD CCL

```

* Ejemplo de conversion de dolar cable a dolar mep

```csv

2025-03-10,2025-03-10,NOTA DE CREDITO U$S,74514,0.0,,0.0,131.5228917676708,2.301650605934239,conv cable a me,USD MEP

2025-03-10,2025-03-10,NOTA DE DEBITOS U$S,84461,0.0,,0.0,-132.0,0.21,conv cable a me,USD CCL

```

* Ejemplo de compra de dolares mep a pesos:

```csv

2025-03-06,2025-03-05,COMPRA PARIDAD,1963771,90.0,AE38,0.6780817173150522,-61.027334430605876,0.895684822612784,,USD MEP

2025-03-18,2025-03-17,VENTA,2305329,-90.0,AE38,0.662132490375172,59.591924149781754,61.88479904791505,,ARS

```

* Ejemplo de venta de dolares ccl a pesos (el ratio entre el cedear de KO y KO.US es de 1:5 por lo que si la venta de KO u otra especie cedear coincide con una compra exterior anterior o posterior en base a la cantidad de acciones llevadas a al ratio de cedear, quiere decir que se hizo una conversion de dolares cable a pesos):

```csv

2026-01-29,2026-01-28,VENTA,1052109,-110.0,KO,21910.176273,2410119.39,2410119.41,,ARS

2026-01-29,2026-01-28,COMPRA EXTERIOR V,1060115,22.0,KO.US,73.185455,-1610.08,41.92,,USD CCL

```

* Ejemplo de venta de dolares mep a pesos:

```csv

2025-03-06,2025-03-05,COMPRA PARIDAD,1963771,90.0,AE38,0.673778,-60.64,0.89,,USD MEP

2025-03-18,2025-03-17,VENTA,2305329,-90.0,AE38,826.824222,74414.18,77277.36,,ARS

```

#### Licitaciones (adquisición de especies en el mercado primario)

- Licitación paridad: Adquisición de un bono en dolares, que paga y amortiza en pesos. Se compra en dolares mep y se paga en pesos y se puede vender en pesos. Ejemplo:

```csv

2024-11-25,2024-11-25,LICITACION PARIDAD,1031177,0.0,,0.0,-202.15,-2.15,SNSBO Sami,USD MEP

2024-11-25,2024-11-25,PAGO DIV,10311770,197.0,SNSBO,101495.736,0.0,263064.66,,ARS

2024-12-02,2024-11-29,VENTA,10454503,-197.0,SNSBO,1104.422234,217571.18,956.2,,ARS

```

- Licitación privada: Adquisición de una especie en pesos. Ejemplo:

```csv

2025-12-18,2025-12-18,LICITACION PRIVADA,10871408,407.0,LK01Q,1485.3757,-604547.91,263734.61,,ARS

```

#### Suscripcion y rescate de fondos comunes de inversión

- Suscripcion: Compra de cuotapartes de un fondo comun de inversion. Ejemplo:

```csv

2023-01-10,2023-01-10,SUSCRIPCION FCI,354521,0.0,RIGAHOR,0.0,-20000.0,64893.97,,ARS

```

- Rescate: Venta de cuotapartes de un fondo comun de inversion. Ejemplo:

```csv

2023-01-25,2023-01-25,LIQUIDACION RESCATE FCI,367769,-317.926599,RIGAHOR,0.0,20433.42,63.92,,ARS

```

- **NOTA**: AL no tener un precio, simplemente se toma el importe de la operacion como valor de la especie.



#### Compra y venta de activos

- Compra normal/trading/exterior v: Es la compra de una especie. Si el origen es ARS, se compra en pesos. Si el origen es USD MEP o USD CCL, se compra en dolares mep o dolares ccl.

- Venta normal/trading/exterior v: es la venta de una especie. Si el origen es ARS, se compra en pesos. Si el origen es USD MEP o USD CCL, se compra en dolares mep o dolares ccl.



#### Pago de dividendos, intereses y amortizaciones

- Son operaciones que depositan dinero liquido en la cuenta en pesos, dolares mep o dolares ccl que no interfieren en los montos de las cuentas corrientes. Solo suman al patrimonio en la moneda correspondiente.



#### Retenciones

- Son operaciones que disminuyen el dinero liquido de la cuenta en pesos, dolares mep o dolares ccl. Estas retenciones se hacen en la cuenta en la moneda correspondiente.

```

Respondeme:

¿Como debería encarar la implementación del código para obtener el resultado deseado que es tener un csv con la evolución de mi patrimonio en las diferentes categorias?

¿Hay inconcistencias en la redación?

¿Que tipo de estructura de datos es la ideal para este tipo de análisis teniendo en cuenta que tengo las cotizaciones de casi todos mis activos exceptuando el de los fondos comunes de inversión y cauciones?

¿Como debería redactar la implementación para que la lógica del código sea fiel a los movimientos reales de mi patrimonio?