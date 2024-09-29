# Inteligencia en finanzas personales
## Descripción
Repositorio de ETLs y análisis de datos para la toma de decisiones en finanzas personales.

## Dependencias
- Python 3.12 minimo
- Las que están en el requirements.txt
- Drivers para los navegadores que se quieran usar
- Alguna que otra biblioteca o extension para ejecutar las celdas de los notebooks (no se cuales son).

### Inialización de entorno
1. Ejecutate este paso a paso en el directorio raíz:
```bash
mkdir data logs drivers
cd data
mkdir bronze silver gold
cd ../
```

2. Te tenes que crear el .env con las variables de entorno que estan en los scripts, van a ser aquellas con tus credenciales de tus brokers o de tu cuenta primary.
```bash
# Archivo .env que se encuentra en el directorio raiz
XBROKER_PASSWORD=123456
XBROKER_USER=usuario

YBROKER_PASSWORD=123456
YBROKER_USER=usuario

XPRIMARY_PASSWORD=123456
XPRIMARY_USER=usuario
XPRIMARY_ACCOUNT=cuenta_comitente

# puse un template.env en donde puede que te quede más claro como tiene que quedar
```
3. Ejecutate esto:
```bash
python3 -m venv venv
# o como carajo quieras que se llama tu entorno virtual
# si estas en linux
source venv/bin/activate
# si estas en windows
venv\Scripts\activate

# despues te intalas los requirements, acordate que tenes que estar en Python 3.12 sino no funca, o por ahí si pero qcyo
pip install -r requirements.txt
```

## Dependencias
Drivers para mozilla
- https://github.com/mozilla/geckodriver/releases

Drivers para chrome
- Selenium no necesita drivers para google chrome


## Estructura de directorios
Tomy, acordate por qué hiciste lo de bronce, plata y oro. Te hicite el que sabias y dividiste los scripts para que un proceso lleve dato de un lugar a otro. Te lo anoto yo (Tomi del pasado) porque se que sos medio idiota (por no decir completamente) y sé que te vas a olvidar de todo. Te quiero mucho <3.
  
