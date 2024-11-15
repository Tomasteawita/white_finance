# activa el entorno virtual que se encuentra en C:\Users\{usuario}\bifi_analytics-etl\venv\scripts\activate
# tomando como parametro el usuario

param (
    [string]$usuario,
    [string]$browser
)

$path_pwd = pwd

echo "Activando entorno virtual desde path absoluto"
cd "C:\Users\$usuario\bifi_analytics-etl\venv\Scripts"
./activate
echo "Entorno virtual activado"

echo "Ejecutando script de python"

cd "C:\Users\$usuario\bifi_analytics-etl\scripts\bronze"

echo "Primero vamos por Bull Market"
python client.py --browser $browser
echo "Segundo vamos por Invertir Online"
python client.py --browser $browser

echo "Desactivando entorno virtual"
deactivate
echo "Entorno virtual desactivado"
echo "Volviendo al path original"
cd $path_pwd