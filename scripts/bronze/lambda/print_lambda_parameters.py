"""Modulo ejecutado dentro de una labda de AWS que imprime el contexto y 
parametros de entrada con el fin de explorarlos."""

import json

def lambda_handler(event, context):
    # Imprime el contexto
    print("Contexto: " + str(context))
    # Imprime el evento
    print("Evento: " + str(event))
    return {
        'statusCode': 200,
        'body': json.dumps('Hello from Lambda!')
    }
