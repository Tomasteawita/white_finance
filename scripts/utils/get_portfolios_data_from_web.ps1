# activa el entorno virtual que se encuentra en C:\Users\{usuario}\portfolios-etl\venv\scripts\activate
# tomando como parametro el usuario

param (
    [string]$usuario,
    [string]$browser
)

# obtengo fecha del día de hoy en formato YYYY-MM-DD
$fecha = Get-Date -Format "yyyy-MM-dd"

$path_pwd = pwd

echo "Activando entorno virtual desde path absoluto"
cd "C:\Users\$usuario\portfolios-etl\venv\Scripts"
./activate
echo "Entorno virtual activado"
echo "RECORDA QUE PRIMERO TENES QUE COPIARTE EL TIPO DE CAMBIO MEP"
echo "Ejecutando script de python"

cd "C:\Users\$usuario\portfolios-etl\scripts\bronze"

echo "Primero vamos por Bull Market"
python client.py --browser $browser
echo "Escribi el tipo de cambio que te anotaste:"
$tipo_cambio = Read-Host "Escribi el tipo de cambio que te anotaste"
echo $tipo_cambio > "C:\Users\$usuario\portfolios-etl\data\bronze\tipo_cambio_$fecha.txt"

echo "Segundo vamos por Invertir Online"
python client.py --browser $browser

echo "Desactivando entorno virtual"
deactivate
echo "Entorno virtual desactivado"
echo "Volviendo al path original"
cd $path_pwd