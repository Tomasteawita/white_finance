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

#### Inicializxar entorno con docker

```docker
docker-compose up --build
```

Este Te da la posibilidad de ejecutar los notebooks y levantar una base de datos postgres, la base de datos postgres la verdad es que es medio al pedo porque no la uso en el  así que en algún momento la voy a sacar.

# Step Functions

La step function que se encuentra en el directorio de pipelines, se ejecuta con un template de eventos de S3, con los siguientes argumentos:

```json
{
    "Records": [
        {
            "eventVersion": "2.0",
            "eventSource": "aws:s3",
            "awsRegion": "{region}",
            "eventTime": "1970-01-01T00:00:00Z",
            "eventName": "ObjectCreated:Put",
            "userIdentity": {
                "principalId": "EXAMPLE"
            },
            "requestParameters": {
                "sourceIPAddress": "127.0.0.1"
            },
            "responseElements": {
                "x-amz-request-id": "EXAMPLE123456789",
                "x-amz-id-2": "EXAMPLE123/5678abcdefghijklambdaisawesome/mnopqrstuvwxyzABCDEFGH"
            },
            "s3": {
                "s3SchemaVersion": "1.0",
                "configurationId": "testConfigRule",
                "bucket": {
                    "name": "witefinance-raw",
                    "ownerIdentity": {
                        "principalId": "EXAMPLE"
                    },
                    "arn": "arn:{partition}:s3:::mybucket"
                },
                "object": {
                    "key": "data/in/cuenta_corriente-YYYYMMDD.csv",
                    "size": 1024,
                    "eTag": "0123456789abcdef0123456789abcdef",
                    "sequencer": "0A1B2C3D4E5F678901"
                }
            }
        }
    ]
}
```

Lo importante es que la clave Records[0][S3][Bucket][Name] y Records[0][S3][object][key] tengan los valores del bucket de la data cruda y la key sea la del data/in
