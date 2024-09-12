# Inteligencia en finanzas personales
## Descripción
Repositorio de ETLs y análisis de datos para la toma de decisiones en finanzas personales.

## Dependencias
- Python 3.12
- Las que están en el requirements.txt
- Drivers para los navegadores que se quieran usar
- Alguna que otra biblioteca o extension para ejecutar las celdas de los notebooks

### Inialización de entorno
1. Te tenes que crear las siguientes carpetas:
```bash
mkdir data logs drivers
```

2. Te tenes que crear el .env con las variables de entorno que estan en los scripts, van a ser aquellas con tus credenciales de tus brokers o de tu cuenta primary.
```bash
# Archivo .env que se encuentra en el directorio raiz
BROKER_PASSWORD=123456
BROKER_USER=usuario

PRIMARY_PASSWORD=123456
PRIMARY_USER=usuario
PRIMARY_ACCOUNT=cuenta_comitente
```
3. Ejecutate esto:
```bash
python3 -m venv venv
# si estas en linux
source venv/bin/activate
# si estas en windows
venv\Scripts\activate
pip install -r requirements.txt
```

## Dependencias
Drivers para mozilla
- https://github.com/mozilla/geckodriver/releases

Drivers para chrome
- Selenium no necesita drivers para google chrome
  
