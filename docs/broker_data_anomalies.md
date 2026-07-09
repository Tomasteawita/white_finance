# Registro de Anomalías de Datos de Brokers (Bull Market / Balanz)

Este documento centraliza el conocimiento empírico sobre cómo los brokers locales exportan la información transaccional y los "quirks" (anomalías) que deben ser manejados defensivamente en los pipelines ETL.

## 1. Operaciones Dólar MEP (Doble Asiento Contable)
### El Problema
Cuando un inversor realiza una operación de Dólar MEP (ej: `COMPRA PARIDAD` o `VENTA PARIDAD`), el broker liquida la operación en dos monedas distintas. Esto provoca que el CSV exportado contenga **dos filas con el mismo campo `Numero`**:
- **Fila 1 (ARS)**: Refleja el movimiento nominal en pesos (muchas veces con `Importe = 0.00`).
- **Fila 2 (USD MEP)**: Refleja el movimiento real de los fondos en especie Dólar MEP.

Recientemente (ej. mediados de 2026), en algunas operaciones de VENTA, el broker comenzó a exportar **solo una fila** (la de USD MEP), perdiendo previsibilidad.

### La Solución (Golden Rules)
- **Deduplicación por `Numero`**: Jamás asumas que la cantidad de nominales se debe sumar/restar ciegamente. Mantén un registro (ej. un `set()`) de los `Numero` ya procesados para no sumar el inventario nominal dos veces.
- **Prohibido el uso de `continue`**: Al iterar el dataframe de transacciones, no utilices `continue` luego de actualizar el saldo líquido (cash) de una fila en USD MEP. Esto provoca que el motor saltee la actualización del inventario de nominales, dejando activos "fantasmas" en la cartera.
- **Control estricto de Saldos ARS**: Las retenciones impositivas y derechos de mercado a veces no aparecen como filas explícitas. Nunca acumules iterativamente `Cash_ARS += importe`. Si la fila reporta la columna `Saldo`, el saldo líquido interno del pipeline debe sobreescribirse con ese valor exacto.

## 2. Fondos Comunes de Inversión (Valuación)
Las suscripciones y rescates de FCIs se reportan como movimientos de `Importe` en pesos.
- **Error Común**: Guardar el "Monto Neto Invertido" como si fuera el saldo actual del FCI, lo cual ignora el rendimiento de las cuotapartes.
- **Buenas Prácticas**: El pipeline debe extraer la *cantidad de cuotapartes* (Nominales) de cada suscripción/rescate y multiplicarla diariamente por el Valor Cuotaparte (VCP) actualizado.
