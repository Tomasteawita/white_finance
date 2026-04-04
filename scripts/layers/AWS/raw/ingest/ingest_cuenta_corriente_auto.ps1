# Script automatizado para procesar cuentas corrientes sin interacción manual
# Uso: .\ingest_cuenta_corriente_auto.ps1 -fecha "2024-01-15" -moneda "PESOS"

param(
    [Parameter(Mandatory=$true)]
    [ValidatePattern('^\d{4}-\d{2}-\d{2}$')]
    [string]$fecha,
    
    [Parameter(Mandatory=$true)]
    [ValidateSet("PESOS", "DOLARES", "DOLARES CABLE")]
    [string]$moneda
)

# --- Configuración ---
$localDir = "withe-finance-ingest"
$s3Bucket = "withefinance-raw"
$s3Prefix = "data/in"
$stateMachineArn = "arn:aws:states:us-east-2:515966533232:stateMachine:WitheFinance-Historical-Profits"
# --------------------

Write-Host "========================================" -ForegroundColor Cyan
Write-Host "[PROCESANDO] cuenta corriente: $moneda" -ForegroundColor Cyan
Write-Host "[FECHA] $fecha" -ForegroundColor Cyan
Write-Host "========================================" -ForegroundColor Cyan

# Convertir fechas
$fecha_dd_mm_yy = Get-Date $fecha -Format "dd-MM-yy"
$fecha_yyyy_mm_dd_formateada = Get-Date $fecha -Format "yyyyMMdd"

Write-Host "Fecha convertida (dd-MM-yy): $fecha_dd_mm_yy" -ForegroundColor Green

# Definir nombres de archivo según tipo de moneda
$excelFileName = "Cuenta Corriente $moneda $fecha_dd_mm_yy.xlsx"

switch ($moneda) {
    "PESOS" {
        $csvFileName = "cuenta_corriente-$fecha_yyyy_mm_dd_formateada.csv"
        $csvFileNameToValidate = "cuenta_corriente"
    }
    "DOLARES" {
        $csvFileName = "cuenta_corriente_dolares-$fecha_yyyy_mm_dd_formateada.csv"
        $csvFileNameToValidate = "cuenta_corriente_dolares"
    }
    "DOLARES CABLE" {
        $csvFileName = "cuenta_corriente_dolares_cable-$fecha_yyyy_mm_dd_formateada.csv"
        $csvFileNameToValidate = "cuenta_corriente_dolares_cable"
    }
}

# Verificar que el archivo Excel exista
$excelPath = "C:\Users\tomas\$localDir\$excelFileName"
if (-not (Test-Path $excelPath)) {
    Write-Host "[ERROR]: No se encontró el archivo '$excelFileName' en $localDir" -ForegroundColor Red
    Write-Host "   Asegúrate de haber copiado el archivo desde Descargas primero." -ForegroundColor Yellow
    exit 1
}

# Activar entorno virtual
Write-Host "[VENV] Activando entorno virtual..." -ForegroundColor Cyan
.\venv\Scripts\activate

# Ejecutar validador y convertir a CSV
Write-Host "[PYTHON] Ejecutando script de Python para validar '$excelFileName'..." -ForegroundColor Cyan
python .\scripts\layers\AWS\raw\ingest\validators\main.py `
    --file_path "C:\Users\tomas\$localDir\$excelFileName" `
    --output_path "C:\Users\tomas\$localDir\$csvFileName" `
    --validator_name "$csvFileNameToValidate"

# Verificar si el CSV fue creado
if (-not (Test-Path "C:\Users\tomas\$localDir\$csvFileName")) {
    Write-Host "[ERROR]: El archivo CSV '$csvFileName' no fue creado." -ForegroundColor Red
    exit 1
} else {
    Write-Host "[OK] Archivo CSV creado: $csvFileName" -ForegroundColor Green
}

# Sincronizar con S3
Write-Host "[S3] Sincronizando con S3..." -ForegroundColor Cyan
aws s3 sync "C:\Users\tomas\$localDir" "s3://$s3Bucket/$s3Prefix/" --exclude "*" --include "cuenta_corriente*.csv"

if ($LASTEXITCODE -ne 0) {
    Write-Host "[ERROR]: La sincronización con S3 falló." -ForegroundColor Red
    exit 1
} else {
    Write-Host "[OK] Sincronización exitosa." -ForegroundColor Green
}

# Identificar archivo más reciente
$latestFile = Get-ChildItem -Path "C:\Users\tomas\$localDir" -Filter "cuenta_corriente*.csv" | 
    Sort-Object LastWriteTime -Descending | 
    Select-Object -First 1

if (-not $latestFile) {
    Write-Host "[WARN] No se encontraron archivos CSV." -ForegroundColor Yellow
    exit 0
}

Write-Host "[FILE] Archivo más reciente: $($latestFile.Name)" -ForegroundColor Green

# Preparar input para Step Function
$s3Key = "$s3Prefix/$($latestFile.Name)"

$inputObject = @{
    Records = @(
        @{
            s3 = @{
                bucket = @{
                    name = $s3Bucket
                }
                object = @{
                    key = $s3Key
                }
            }
        }
    )
}

$jsonInputRaw = $inputObject | ConvertTo-Json -Depth 5
$jsonInputCompressed = $inputObject | ConvertTo-Json -Depth 5 -Compress
$jsonInputEscaped = $jsonInputCompressed.Replace('"', '\"')

Write-Host "[JSON] JSON de entrada preparado:" -ForegroundColor Cyan
Write-Host $jsonInputRaw -ForegroundColor Gray

# Ejecutar Step Function
Write-Host "[STEPFUNC] Iniciando Step Function..." -ForegroundColor Cyan
$executionArn = aws stepfunctions start-execution `
    --state-machine-arn $stateMachineArn `
    --input $jsonInputEscaped `
    --query "executionArn" `
    --output text

if ($LASTEXITCODE -eq 0) {
    Write-Host "[OK] Step Function iniciada." -ForegroundColor Green
    Write-Host "   Execution ARN: $executionArn"
} else {
    Write-Host "[ERROR]: Falló el inicio de la Step Function." -ForegroundColor Red
    exit 1
}

# Limpiar archivos temporales
Remove-Item "C:\Users\tomas\$localDir\cuenta_corriente*.csv" -Force -ErrorAction SilentlyContinue
Remove-Item "C:\Users\tomas\$localDir\$excelFileName" -Force -ErrorAction SilentlyContinue
Write-Host "[CLEANUP] Archivos locales eliminados." -ForegroundColor Green

deactivate

# Esperar procesamiento
Write-Host "[WAIT] Esperando 30 segundos para que la Step Function procese..." -ForegroundColor Yellow
Start-Sleep -Seconds 30

# Descargar archivo histórico actualizado
Write-Host "[DOWNLOAD] Descargando archivo histórico actualizado..." -ForegroundColor Cyan

switch ($moneda) {
    "PESOS" {
        $historicalFileName = "cuenta_corriente_historico.csv"
        aws s3 cp "s3://whitefinance-analytics/profit.csv" `
            "C:\Users\tomas\white_finance\data\analytics\profit.csv"
    }
    "DOLARES" {
        $historicalFileName = "cuenta_corriente_dolares_historico.csv"
    }
    "DOLARES CABLE" {
        $historicalFileName = "cuenta_corriente_dolares_cable_historico.csv"
    }
}

aws s3 cp "s3://withefinance-integrated/cuenta_corriente_historico/$historicalFileName" `
    "C:\Users\tomas\white_finance\data\analytics\$historicalFileName"

Write-Host "========================================" -ForegroundColor Green
Write-Host "[OK] Proceso completado para $moneda" -ForegroundColor Green
Write-Host "========================================" -ForegroundColor Green
