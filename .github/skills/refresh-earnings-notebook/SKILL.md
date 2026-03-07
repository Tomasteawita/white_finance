# Skill: Actualizar Ganancias Realizadas

## Descripción
Esta skill actualiza las visualizaciones del notebook `ganancias_realizadas.ipynb` procesando las cuentas corrientes descargadas del broker.

## Precondiciones
- Tener descargados los archivos de cuenta corriente desde Bull Market en la carpeta de **Descargas**
- Los archivos deben seguir el formato: `Cuenta Corriente (DOLARES|DOLARES CABLE|PESOS) dd-MM-yy.xlsx`
- Tener configurado AWS CLI con credenciales válidas
- Tener el entorno virtual activado

## Inputs Requeridos
| Parámetro | Formato | Descripción | Ejemplo |
|-----------|---------|-------------|---------|
| `fecha` | YYYY-MM-DD | Fecha de la cuenta corriente a procesar | 2024-01-15 |

## Flujo de Ejecución

### Paso 1: Copiar archivos desde Descargas
Copia los 3 archivos de cuenta corriente desde `C:\Users\tomas\Downloads` hacia `C:\Users\tomas\withe-finance-ingest`:
- `Cuenta Corriente PESOS dd-MM-yy.xlsx`
- `Cuenta Corriente DOLARES dd-MM-yy.xlsx`
- `Cuenta Corriente DOLARES CABLE dd-MM-yy.xlsx`

### Paso 2: Procesar cada cuenta corriente
Ejecuta el script de ingesta para cada tipo de moneda:
1. PESOS
2. DOLARES
3. DOLARES CABLE

### Paso 3: Actualizar visualizaciones
Ejecuta las celdas del notebook `ganancias_realizadas.ipynb` para regenerar los gráficos.

---

## Ejecución Automática (YAML)

```yaml
name: refresh-earnings-notebook
description: Actualiza las ganancias realizadas procesando las cuentas corrientes

inputs:
  fecha:
    description: "Fecha de la cuenta corriente (formato YYYY-MM-DD)"
    required: true
    type: string
    pattern: "^\\d{4}-\\d{2}-\\d{2}$"

steps:
  - name: Validar fecha
    action: validate
    with:
      value: ${{ inputs.fecha }}
      pattern: "^\\d{4}-\\d{2}-\\d{2}$"
      error_message: "La fecha debe estar en formato YYYY-MM-DD"

  - name: Copiar cuentas corrientes desde Descargas
    action: run_terminal
    with:
      command: |
        $fecha = "${{ inputs.fecha }}"
        $fecha_dd_mm_yy = Get-Date $fecha -Format "dd-MM-yy"
        
        $currencies = @("PESOS", "DOLARES", "DOLARES CABLE")
        $sourceDir = "C:\Users\tomas\Downloads"
        $destDir = "C:\Users\tomas\withe-finance-ingest"
        
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

  - name: Procesar cuenta corriente PESOS
    action: run_terminal
    with:
      command: |
        cd C:\Users\tomas\white_finance
        .\scripts\raw\ingest\ingest_cuenta_corriente_auto.ps1 -fecha "${{ inputs.fecha }}" -moneda "PESOS"

  - name: Procesar cuenta corriente DOLARES
    action: run_terminal
    with:
      command: |
        cd C:\Users\tomas\white_finance
        .\scripts\raw\ingest\ingest_cuenta_corriente_auto.ps1 -fecha "${{ inputs.fecha }}" -moneda "DOLARES"

  - name: Procesar cuenta corriente DOLARES CABLE
    action: run_terminal
    with:
      command: |
        cd C:\Users\tomas\white_finance
        .\scripts\raw\ingest\ingest_cuenta_corriente_auto.ps1 -fecha "${{ inputs.fecha }}" -moneda "DOLARES CABLE"

  - name: Notificar finalización
    action: notify
    with:
      message: "✅ Cuentas corrientes procesadas. Ejecuta el notebook ganancias_realizadas.ipynb para ver las visualizaciones actualizadas."
```

---

## Script de Ejecución Manual

Si prefieres ejecutar manualmente, usa el siguiente comando en PowerShell:

```powershell
# Reemplaza YYYY-MM-DD con la fecha deseada
.\scripts\raw\ingest\ingest_cuenta_corriente_auto.ps1 -fecha "2024-01-15" -moneda "PESOS"
.\scripts\raw\ingest\ingest_cuenta_corriente_auto.ps1 -fecha "2024-01-15" -moneda "DOLARES"
.\scripts\raw\ingest\ingest_cuenta_corriente_auto.ps1 -fecha "2024-01-15" -moneda "DOLARES CABLE"
```

---

## Troubleshooting

| Error | Causa | Solución |
|-------|-------|----------|
| Archivo no encontrado | El archivo no está en Descargas | Verificar que el nombre siga el formato exacto |
| Error de AWS | Credenciales expiradas | Ejecutar `aws configure` o renovar credenciales |
| Step Function falla | Datos inválidos en el CSV | Revisar el archivo Excel original |

---

## Archivos Relacionados
- Script de ingesta: `scripts/raw/ingest/ingest_cuenta_corriente.ps1`
- Script automatizado: `scripts/raw/ingest/ingest_cuenta_corriente_auto.ps1`
- Notebook: `notebooks/ganancias_realizadas.ipynb`
- Datos procesados: `data/analytics/`
