# --- Configuración ---
# Directorio local donde se encuentra tu archivo CSV
$localDir = "withe-finance-ingest"

# Bucket y prefijo (path) en S3 donde se subirá el archivo
$s3Bucket = "withefinance-raw"
$s3Prefix = "data/in"

# ARN completo de tu máquina de estado
$stateMachineArn = "arn:aws:states:us-east-2:515966533232:stateMachine:WitheFinance-Historical-Profits"
# --------------------
# Le pido la fecha al usuario
$fecha_yyyy_mm_dd = Read-Host "Ingrese la fecha (formato YYYY-MM-DD)"
# Paso la fecha de YYYY-MM-DD a dd-MM-YY
$fecha_dd_mm_yy = Get-Date $fecha_yyyy_mm_dd -Format "dd-MM-yy"
# paso Fecha de YYYY-MM-DD a YYYYMMDD
$fecha_yyyy_mm_dd_formateada = Get-Date $fecha_yyyy_mm_dd -Format "yyyyMMdd"
# imprimo ambas fechas para confirmar
Write-Host "Fecha ingresada (YYYY-MM-DD): $fecha_yyyy_mm_dd"
Write-Host "Fecha convertida (YYYYMMDD): $fecha_yyyy_mm_dd_formateada" -ForegroundColor Green
Write-Host "Fecha convertida (dd-MM-yy): $fecha_dd_mm_yy" -ForegroundColor Green

# Defino el nombre del archivo Excel y CSV
$excelFileName = "Cuenta Corriente PESOS $fecha_dd_mm_yy.xlsx"
$csvFileName = "cuenta_corriente-$fecha_yyyy_mm_dd_formateada.csv"


.\venv\Scripts\activate
Write-Host "📊 Ejecutando script de Python para validar el excel y pasarlo a csv '$excelFileName' a '$csvFileName'..."
python .\scripts\raw\ingest\validators\main.py `
    --file_path "C:\Users\tomas\$localDir\$excelFileName" `
    --output_path "C:\Users\tomas\$localDir\$csvFileName" `
    --validator_name "cuenta_corriente"
# Verificar si el archivo CSV fue creado
if (-not (Test-Path "C:\Users\tomas\$localDir\$csvFileName")) {
    Write-Host "❌ Error: El archivo CSV '$csvFileName' no fue creado. Verifique el script de validación." -ForegroundColor Red
    exit 1
} else {
    Write-Host "✅ Archivo CSV creado exitosamente: $csvFileName" -ForegroundColor Green
}
# Paso 1: Sincronizar el archivo CSV con S3
Write-Host "🚀 Sincronizando archivos desde 'C:\Users\tomas\$localDir' hacia 's3://$s3Bucket/$s3Prefix/'..." -ForegroundColor Cyan
aws s3 sync "C:\Users\tomas\$localDir" "s3://$s3Bucket/$s3Prefix/" --exclude "*" --include "cuenta_corriente-*.csv"

# Verificar si la sincronización fue exitosa (el código 0 indica éxito)
if ($LASTEXITCODE -ne 0) {
    Write-Host "❌ Error: La sincronización con S3 falló." -ForegroundColor Red
    exit 1
} else {
    Write-Host "✅ Sincronización exitosa." -ForegroundColor Green
}

# Paso 2: Identificar el archivo más reciente para usar como input
# Get-ChildItem para buscar, Sort-Object para ordenar y Select-Object para tomar el último
$latestFile = Get-ChildItem -Path "C:\Users\tomas\$localDir" -Filter "cuenta_corriente-*.csv" | Sort-Object LastWriteTime -Descending | Select-Object -First 1

if (-not $latestFile) {
    Write-Host "⚠️ No se encontraron archivos 'cuenta_corriente-*.csv'. No se iniciará la Step Function." -ForegroundColor Yellow
    exit 0
}

Write-Host "📄 Archivo más reciente identificado: $($latestFile.Name)" -ForegroundColor Green

# Paso 3: Preparar el objeto de entrada y convertirlo a JSON
# Este método es más seguro y nativo de PowerShell que manipular texto.
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

# Convierte el objeto de PowerShell a una cadena JSON
$jsonInput = $inputObject | ConvertTo-Json -Depth 5

# imprimir el JSON para depuración
Write-Host "📝 Objeto de entrada JSON preparado:" -ForegroundColor Cyan
Write-Host $jsonInput -ForegroundColor Gray

# Paso 4: Ejecutar la Step Function
Write-Host "⚙️ Iniciando la ejecución de la Step Function..." -ForegroundColor Cyan
$executionArn = aws stepfunctions start-execution `
    --state-machine-arn $stateMachineArn `
    --input $jsonInput `
    --query "executionArn" `
    --output text

if ($LASTEXITCODE -eq 0) {
    Write-Host "✅ ¡Éxito! Step Function iniciada." -ForegroundColor Green
    Write-Host "   Execution ARN: $executionArn"
}
else {
    Write-Host "❌ Error: Falló el inicio de la Step Function." -ForegroundColor Red
    exit 1
}

Remove-Item "C:\Users\tomas\$localDir\cuenta_corriente-*.csv" -Force -ErrorAction SilentlyContinue
Remove-Item "C:\Users\tomas\$localDir\Cuenta Corriente PESOS *.xlsx" -Force -ErrorAction SilentlyContinue
Write-Host "🗑️ Archivos locales eliminados." -ForegroundColor Green
deactivate
Write-Host "✅ Proceso completado." -ForegroundColor Green