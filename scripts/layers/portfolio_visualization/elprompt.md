# Visualización de patrimonio en bolsa
## Contexto
Gestionamos el patrimonio de un cliente dentro de un broker de bolsa regulado por la comisión nacional de valores en Argentina. El cuente tiene tres tipos de cuentas:
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

## Implementación
Genera un script que lea el archivo cuentas_unificadas_usd_sorted.csv y genere un archivo csv con la estructura solicitada. Implementando una lógica que cumpla con las siguientes reglas:
```json
{
    "RECIBO DE COBRO": "Aumenta el valor en Cash_Total_USD",
    "SUSCRIPCION FCI": "Disminuye el valor en Cash_Total_USD y lo aumenta en Total_Growth_Valuation",
    "COMPRA NORMAL": "Disminuye el valor en Cash_Total_USD y lo aumenta en Total_Growth_Valuation o Total_Safe_Valuation",
    "ORDEN DE PAGO": "Disminuye el valor en Cash_Total_USD",
    "LIQUIDACION RESCATE FCI": "Aumenta el valor en Cash_Total_USD y lo disminuye en Total_Growth_Valuation",
    "VENTA": "Aumenta el valor en Cash_Total_USD y disminuye en Total_Growth_Valuation o Total_Safe_Valuation",
    "ORD PAGO DOLARES": "Disminuye el valor en Cash_Total_USD",
    "VENTA PARIDAD": "Aumenta el valor en Cash_Total_USD y disminuye en Total_Growth_Valuation o Total_Safe_Valuation",
    "REC COBRO DOLARES": "Aumenta el valor en Cash_Total_USD",
    "COMPRA PARIDAD": "Disminuye el valor en Cash_Total_USD y lo aumenta en Total_Growth_Valuation o Total_Safe_Valuation",
    "PAGO DIV": "Aumenta el valor en Cash_Total_USD",
    "DIVIDENDOS": "Aumenta el valor en Cash_Total_USD",
    "CREDITO DERMERC-": "Aumenta el valor en Cash_Total_USD",
    "COMPRA TRADING": "Disminuye el valor en Cash_Total_USD y lo aumenta en Total_Growth_Valuation o Total_Safe_Valuation",
    "VENTA TRADING": "Aumenta el valor en Cash_Total_USD y disminuye en Total_Growth_Valuation o Total_Safe_Valuation",
    "RETENCION": "Disminuye el valor en Cash_Total_USD",
    "LICITACION PARIDAD": "Disminuye el valor en Cash_Total_USD y lo aumenta en Total_Growth_Valuation o Total_Safe_Valuation",
    "VENTA EXTERIOR V": "Aumenta el valor en Cash_Total_USD y disminuye en Total_Growth_Valuation o Total_Safe_Valuation",
    "RETENCION DOLARES": "Disminuye el valor en Cash_Total_USD",
    "DIVIDENDO TESORO": "Aumenta el valor en Cash_Total_USD",
    "COMPRA EXTERIOR V": "Disminuye el valor en Cash_Total_USD y lo aumenta en Total_Growth_Valuation o Total_Safe_Valuation",
    "NOTA DE CREDITO U$S": "Aumenta el valor en Cash_Total_USD",
    "NOTA DE DEBITOS U$S": "Disminuya el valor en Cash_Total_USD",
    "RETENCION DOLAR MEP": "Disminuya el valor en Cash_Total_USD",
    "COMPRA CAUCION CONTADO": "Disminuya el valor en Cash_Total_USD y lo aumenta en Total_Safe_Valuation",
    "VENTA CAUCION TERMINO": "Aumenta el valor en Cash_Total_USD y lo disminuye en Total_Safe_Valuation",
    "LICITACION PRIVADA": "Disminuye el valor en Cash_Total_USD y lo aumenta en Total_Growth_Valuation o Total_Safe_Valuation",
    "RENTA Y AMORTIZ": "Aumenta el valor en Cash_Total_USD"
}
```
Siendo cada clave del json un tipo de operación que está marcado en la columna "Comprobante" del csv cuentas_unificadas_usd_sorted.csv. Y cada valor del json es una descripción de cómo afecta a cada columna del csv cuentas_unificadas_usd_sorted.csv.
El script debe traerse desde la base de datos postgres la siguiente información:
```sql
SELECT hp.date, ticker, 
	case when "source" <> 'YFinance_USD' then "close" / ccl end as close,
	"source",
	ccl
FROM earnings.historical_prices hp
left join earnings.ccl_mep cm 
on hp.date = cm."date" ;
```




```python
ratios_cedear = {
            'KO': 5.0, 'SPY': 20.0, 'QQQ': 20.0, 'AAPL': 10.0,
            'GOOGL': 58.0, 'MSFT': 30.0, 'TSLA': 15.0, 'MELI': 120.0,
            'LLY': 56.0, 'META': 24.0, 'VIST': 3.0, 'AMZN': 144.0,
            'NVDA': 24.0, 'NFLX': 60.0, 'TLT': 1.0, 'SH': 8.0,
            'ARGT': 1.0, 'XLP': 1.0, 'SHY': 1.0, 'ADBE': 44.0,
            'ARKK': 10.0, 'ASML': 146.0, 'BBD': 1.0, 'BIOX': 1.0,
            'COIN': 27.0, 'ERIC': 2.0, 'HMY': 1.0, 'LAR': 1.0,
            'PAAS': 3.0, 'PSQ': 8.0, 'SAN': 0.25, 'UNH': 33.0, 'VALE': 2.0,
            "
        }
```

```json
{
    "RECIBO DE COBRO": "Aumenta el valor en Cash_Total_USD",
    "SUSCRIPCION FCI": "Disminuye el valor en Cash_Total_USD y lo aumenta en Total_Growth_Valuation",
    "COMPRA NORMAL": "Disminuye el valor en Cash_Total_USD y lo aumenta en Total_Growth_Valuation o Total_Safe_Valuation",
    "ORDEN DE PAGO": "Disminuye el valor en Cash_Total_USD",
    "LIQUIDACION RESCATE FCI": "Aumenta el valor en Cash_Total_USD y lo disminuye en Total_Growth_Valuation",
    "VENTA": "Aumenta el valor en Cash_Total_USD y disminuye en Total_Growth_Valuation o Total_Safe_Valuation",
    "ORD PAGO DOLARES": "Disminuye el valor en Cash_Total_USD",
    "VENTA PARIDAD": "Aumenta el valor en Cash_Total_USD y disminuye en Total_Growth_Valuation o Total_Safe_Valuation",
    "REC COBRO DOLARES": "Aumenta el valor en Cash_Total_USD",
    "COMPRA PARIDAD": "Disminuye el valor en Cash_Total_USD y lo aumenta en Total_Growth_Valuation o Total_Safe_Valuation",
    "PAGO DIV": "Aumenta el valor en Cash_Total_USD",
    "DIVIDENDOS": "Aumenta el valor en Cash_Total_USD",
    "CREDITO DERMERC-": "Aumenta el valor en Cash_Total_USD",
    "COMPRA TRADING": "Disminuye el valor en Cash_Total_USD y lo aumenta en Total_Growth_Valuation o Total_Safe_Valuation",
    "VENTA TRADING": "Aumenta el valor en Cash_Total_USD y disminuye en Total_Growth_Valuation o Total_Safe_Valuation",
    "RETENCION": "Disminuye el valor en Cash_Total_USD",
    "LICITACION PARIDAD": "Disminuye el valor en Cash_Total_USD y lo aumenta en Total_Growth_Valuation o Total_Safe_Valuation",
    "VENTA EXTERIOR V": "Aumenta el valor en Cash_Total_USD y disminuye en Total_Growth_Valuation o Total_Safe_Valuation",
    "RETENCION DOLARES": "Disminuye el valor en Cash_Total_USD",
    "DIVIDENDO TESORO": "Aumenta el valor en Cash_Total_USD",
    "COMPRA EXTERIOR V": "Disminuye el valor en Cash_Total_USD y lo aumenta en Total_Growth_Valuation o Total_Safe_Valuation",
    "NOTA DE CREDITO U$S": "Aumenta el valor en Cash_Total_USD",
    "NOTA DE DEBITOS U$S": "Disminuya el valor en Cash_Total_USD",
    "RETENCION DOLAR MEP": "Disminuya el valor en Cash_Total_USD",
    "COMPRA CAUCION CONTADO": "Disminuya el valor en Cash_Total_USD y lo aumenta en Total_Safe_Valuation",
    "VENTA CAUCION TERMINO": "Aumenta el valor en Cash_Total_USD y lo disminuye en Total_Safe_Valuation",
    "LICITACION PRIVADA": "Disminuye el valor en Cash_Total_USD y lo aumenta en Total_Growth_Valuation o Total_Safe_Valuation",
    "RENTA Y AMORTIZ": "Aumenta el valor en Cash_Total_USD"
}
```
En el caso de que 