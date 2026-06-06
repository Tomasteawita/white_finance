# Módulo de Procesamiento Balanz (FCIs y Cuentas Corrientes)

Este módulo contiene los pipelines encargados de procesar, valorizar y auditar el patrimonio de los clientes a partir de la operatoria de Fondos Comunes de Inversión extraídos de la Cuenta Corriente del broker Balanz.

## ⚙️ Entorno Virtual
Toda ejecución debe realizarse desde el entorno virtual local del proyecto. Abre una terminal en la raíz de `white_finance` y actívalo:
```powershell
.\venv\Scripts\activate
```

---

## 1. Extracción de Cotizaciones (CNV)
**Script:** `extraction_fci_cnv.py`

Se encarga de parsear los archivos Excel nativos que reporta la CNV. Hace un mapeo dinámico escaneando todas las carpetas de los clientes (`maps_fci.json`) para identificar los fondos a procesar y extrae el **Valor histórico real de las cuotapartes** de cada uno.

**Ejecución:**
Asegúrate de estar parado en la raíz del proyecto (`C:\Users\tomas\white_finance`) y ejecuta:
```bash
python scripts\layers\balanz\extraction_fci_cnv.py
```
**Output:** El maestro histórico consolidado se guardará en `data/analytics/cotizaciones/fci_quotes_historico.csv`.

---

## 2. Motor de Evolución del Patrimonio
**Script:** `client_portfolio_evolution.py`

Es el motor Mark-to-Market. Lee las Suscripciones y Rescates de la Cuenta Corriente del broker en orden cronológico para inferir la compra/venta real de cuotapartes (Nominales). Combina esta información con:
- Precios extraídos de la CNV.
- Tipo de Cambio Contado con Liquidación (CCL) histórico (PostgreSQL).
- Inflación (IPC Mensual proyectado a diario) (PostgreSQL).

**Ejecución:**
El script recibe como **argumento dinámico** el nombre de la carpeta del cliente ubicada dentro de `data/balanz/`.

Para ejecutar un cálculo específico para el cliente `ARCE_ZULMA_ELIZABET`:
```bash
python scripts\layers\balanz\client_portfolio_evolution.py ARCE_ZULMA_ELIZABET
```
*(Nota: si lo ejecutas sin argumentos, tomará por default el de ARCE_ZULMA_ELIZABET).*

**Output:** El archivo auditado y proyectado será generado en `data/balanz/{CLIENTE}/reports/evolucion_{cliente}.csv`.

---

## 📜 Logs y Auditoría
Cualquier evento del sistema (éxitos, cálculos, faltantes de precios de cotización de un día o errores) queda registrado automáticamente en un archivo global a nivel empresarial en:
`logs/balanz_pipelines.log`

## Obtención de datos
1. Entrar a https://www.cnv.gov.ar/SitioWeb/FondosComunesInversion/CuotaPartes y entrar al link de las fechas en las que se quiere obtener las cotizaciones de los fondos comunes de inversión.
2. Abrir en pestañas separadas, una por dia.
3. Descargar el excel correspondiente a cada dia
4. Guardarlos en la carpeta data/analytics/cotizaciones/FCIs_cnv.gov.ar_SitioWeb_FondosComunesInversion_CuotaPartes

