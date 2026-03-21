---
description: /refresh-earnings - Actualiza las ganancias realizadas procesando cuentas corrientes
---
Este workflow automatiza la búsqueda, copiado y procesamiento de las cuentas corrientes desde la carpeta de descargas del usuario hacia el directorio de ingesta, utilizando PowerShell.

1. **Solicitar Fecha:** Pídele al usuario la fecha de la cuenta corriente a procesar en formato `YYYY-MM-DD` (ej. 2024-01-15). Verifica que se obtenga este valor antes de continuar.

// turbo
2. **Copiar archivos desde Descargas:**
Ejecuta el siguiente comando en PowerShell. Asegúrate de reemplazar `<FECHA_YYYY_MM_DD>` por la fecha proporcionada por el usuario (el script se encargará de darle el formato adecuado para buscar el archivo):

```pwsh
$fecha = "<FECHA_YYYY_MM_DD>"
$fecha_dd_mm_yy = (Get-Date $fecha).ToString("dd-MM-yy")
$currencies = @("PESOS", "DOLARES", "DOLARES CABLE")
$sourceDir = "C:\Users\tomas\Downloads"
$destDir = "C:\Users\tomas\withe-finance-ingest"

# Asegurar que el directorio destino existe
if (-not (Test-Path $destDir)) { New-Item -ItemType Directory -Force -Path $destDir }

foreach ($currency in $currencies) {
    $fileName = "Cuenta Corriente $currency $fecha_dd_mm_yy.xlsx"
    $sourcePath = Join-Path $sourceDir $fileName
    
    if (Test-Path $sourcePath) {
        Copy-Item -Path $sourcePath -Destination $destDir -Force
        Write-Host "✅ Copiado: $fileName" -ForegroundColor Green
    } else {
        Write-Host "⚠️ No encontrado: $fileName" -ForegroundColor Yellow
    }
}
```

// turbo
3. **Procesar Cuentas Corrientes:**
Ejecuta los scripts de ingesta para las tres distintas monedas utilizando los archivos copiados. Reemplaza `<FECHA_YYYY_MM_DD>` con la fecha original solicitada en el Paso 1:

```pwsh
cd C:\Users\tomas\white_finance
.\scripts\raw\ingest\ingest_cuenta_corriente_auto.ps1 -fecha "<FECHA_YYYY_MM_DD>" -moneda "PESOS"
.\scripts\raw\ingest\ingest_cuenta_corriente_auto.ps1 -fecha "<FECHA_YYYY_MM_DD>" -moneda "DOLARES"
.\scripts\raw\ingest\ingest_cuenta_corriente_auto.ps1 -fecha "<FECHA_YYYY_MM_DD>" -moneda "DOLARES CABLE"
```

4. **Notificar Finalización:**
Informa al usuario que los archivos han sido procesados correctamente con la fecha indicada y recuérdale explícitamente que debe ejecutar y actualizar el notebook `ganancias_realizadas.ipynb` para visualizar los datos actualizados.
