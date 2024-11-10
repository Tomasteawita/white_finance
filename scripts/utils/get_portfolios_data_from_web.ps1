# activa el entorno virtual que se encuentra en C:\Users\{usuario}\bifi_analytics-etl\venv\scripts\activate
# tomando como parametro el usuario

param (
    [string]$usuario,
    [string]$browser
)

# Activa el entorno virtual
echo "Activando entorno virtual desde path absoluto"
cd "C:\Users\$usuario\bifi_analytics-etl\venv\Scripts"
./activate

# Ejecuta el script de python
echo "Ejecutando script de python"

cd "C:\Users\$usuario\bifi_analytics-etl\scripts\bronze"

python client.py --browser $browser
python client.py --browser $browser

deactivate

cd "C:\Users\$usuario\bifi_analytics-etl\scripts\utils"