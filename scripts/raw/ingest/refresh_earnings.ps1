# Script maestro para actualizar ganancias realizadas
# Ejecuta todo el flujo: copia archivos + procesa las 3 cuentas corrientes
#
# Uso: .\refresh_earnings.ps1 -fecha "2024-01-15"

param(
    [Parameter(Mandatory=$true)]
    [ValidatePattern('^\d{4}-\d{2}-\d{2}$')]
    [string]$fecha
)

$ErrorActionPreference = "Stop"

Write-Host "╔════════════════════════════════════════════════════════════╗" -ForegroundColor Cyan
Write-Host "║     ACTUALIZACIÓN DE GANANCIAS REALIZADAS                  ║" -ForegroundColor Cyan
Write-Host "║     Fecha: $fecha                                    ║" -ForegroundColor Cyan
Write-Host "╚════════════════════════════════════════════════════════════╝" -ForegroundColor Cyan
Write-Host ""

# Configuración
$fecha_dd_mm_yy = Get-Date $fecha -Format "dd-MM-yy"
$sourceDir = "C:\Users\tomas\Downloads"
$destDir = "C:\Users\tomas\withe-finance-ingest"
$currencies = @("PESOS", "DOLARES", "DOLARES CABLE")

# ═══════════════════════════════════════════════════════════════
# PASO 1: Copiar archivos desde Descargas
# ═══════════════════════════════════════════════════════════════
Write-Host "📁 PASO 1: Copiando archivos desde Descargas..." -ForegroundColor Yellow
Write-Host "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━" -ForegroundColor Gray

$archivosEncontrados = @()

foreach ($currency in $currencies) {
    $fileName = "Cuenta Corriente $currency $fecha_dd_mm_yy.xlsx"
    $sourcePath = Join-Path $sourceDir $fileName
    
    if (Test-Path $sourcePath) {
        Copy-Item -Path $sourcePath -Destination $destDir -Force
        Write-Host "   ✅ Copiado: $fileName" -ForegroundColor Green
        $archivosEncontrados += $currency
    } else {
        Write-Host "   ⚠️ No encontrado: $fileName" -ForegroundColor Yellow
    }
}

if ($archivosEncontrados.Count -eq 0) {
    Write-Host ""
    Write-Host "❌ No se encontró ningún archivo de cuenta corriente." -ForegroundColor Red
    Write-Host "   Verifica que los archivos estén en: $sourceDir" -ForegroundColor Red
    Write-Host "   Con formato: 'Cuenta Corriente (PESOS|DOLARES|DOLARES CABLE) $fecha_dd_mm_yy.xlsx'" -ForegroundColor Red
    exit 1
}

Write-Host ""
Write-Host "📋 Se encontraron $($archivosEncontrados.Count) de 3 archivos" -ForegroundColor Cyan

# ═══════════════════════════════════════════════════════════════
# PASO 2: Procesar cada cuenta corriente
# ═══════════════════════════════════════════════════════════════
Write-Host ""
Write-Host "⚙️ PASO 2: Procesando cuentas corrientes..." -ForegroundColor Yellow
Write-Host "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━" -ForegroundColor Gray

$procesados = 0
$errores = @()

foreach ($currency in $archivosEncontrados) {
    Write-Host ""
    Write-Host "▶️ Procesando: $currency" -ForegroundColor Cyan
    
    try {
        & "$PSScriptRoot\ingest_cuenta_corriente_auto.ps1" -fecha $fecha -moneda $currency
        $procesados++
        Write-Host "   ✅ $currency procesado correctamente" -ForegroundColor Green
    }
    catch {
        Write-Host "   ❌ Error procesando $currency : $_" -ForegroundColor Red
        $errores += $currency
    }
}

# ═══════════════════════════════════════════════════════════════
# RESUMEN FINAL
# ═══════════════════════════════════════════════════════════════
Write-Host ""
Write-Host "╔════════════════════════════════════════════════════════════╗" -ForegroundColor Green
Write-Host "║                    RESUMEN DE EJECUCIÓN                    ║" -ForegroundColor Green
Write-Host "╚════════════════════════════════════════════════════════════╝" -ForegroundColor Green
Write-Host ""
Write-Host "   📅 Fecha procesada: $fecha" -ForegroundColor White
Write-Host "   ✅ Procesados correctamente: $procesados de $($archivosEncontrados.Count)" -ForegroundColor $(if ($procesados -eq $archivosEncontrados.Count) { "Green" } else { "Yellow" })

if ($errores.Count -gt 0) {
    Write-Host "   ❌ Errores en: $($errores -join ', ')" -ForegroundColor Red
}

Write-Host ""
Write-Host "📊 Para ver las visualizaciones actualizadas:" -ForegroundColor Cyan
Write-Host "   1. Abre el notebook: notebooks/ganancias_realizadas.ipynb" -ForegroundColor White
Write-Host "   2. Ejecuta todas las celdas (Ctrl+Shift+Enter)" -ForegroundColor White
Write-Host ""
