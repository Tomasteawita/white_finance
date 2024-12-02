"""
Documentacion de inicio de sesion ejemplo:
https://api.invertironline.com/Help/Autenticacion

swagger:
https://api.invertironline.com/swagger/ui/index#!/Token/Token_Token


"""
import requests
import boto3

def first_bearer_token():
    # Replace with your actual username and password
    username = "MIUSUARIO" # gmail enrealidad
    password = "MICONTRASEÑA"
    
    # Data to be sent in the request body
    data = {
        "username": username,
        "password": password,
        "grant_type": "password"
    }
    
    # Headers for the request
    headers = {
        "Content-Type": "application/x-www-form-urlencoded"
    }
    
    # URL for the token endpoint
    url = "https://api.invertironline.com/token"
    
    # Send the POST request
    response = requests.post(url, headers=headers, data=data)
    
    # Check for successful response
    if response.status_code == 200:
        # Extract the access token from the response (assuming JSON format)
        data = response.json()
        access_token = data.get("access_token")
        print(f"Access Token: {access_token}")
    else:
        print(f"Error getting token: {response.status_code} - {response.text}")

def refresh_token(refresh_token, base_url="https://api.invertironline.com"):
    """
    Renueva el token de acceso utilizando el refresh token.
  
    Args:
      refresh_token: El token de refresco.
      base_url: La URL base de la API de Invertir Online.
  
    Returns:
      El nuevo token de acceso si la solicitud es exitosa, de lo contrario, None.
    """
  
    url = f"{base_url}/token"
    headers = {
        "Content-Type": "application/x-www-form-urlencoded"
    }
    data = {
        "refresh_token": refresh_token,
        "grant_type": "refresh_token"
    }
  
    response = requests.post(url, headers=headers, data=data)
  
    if response.status_code == 200:
        return response.json().get("access_token")
    else:
        print(f"Error   
   al renovar el token: {response.status_code} - {response.text}")
        return None


def get_refresh_token_from_secrets_manager():
  secret_name = "my-refresh-token"
  region_name = "us-east-1"

  session = boto3.session.Session()
  client = session.client(
      service_name='secretsmanager',
      region_name=region_name   

  )

  try:
      get_secret_value_response = client.get_secret_value(
          SecretId=secret_name
      )
  except ClientError as   
 e:
      if e.response['Error']['Code'] == 'DecryptionFailureException':
          # Secrets Manager can't decrypt the protected secret text using the provided KMS key.   

          # Deal with the exception here.
          raise e
      elif e.response['Error']['Code'] == 'InternalServiceError':
          # An error occurred on the server side.
          # Deal with the exception here.   

          raise e
      elif e.response['Error']['Code'] == 'InvalidParameterException':
          # You provided an invalid value for a parameter.
          # Deal with the exception here.   

          raise e
      elif e.response['Error']['Code'] == 'InvalidRequestException':
          # You provided a malformed request.
          # Deal with the exception here.
          raise e
      elif e.response['Error']['Code'] == 'ResourceNotFoundException':
          # We can't find the resource that you asked for.
          # Deal with the exception here.   

          raise e
  else:
      # Decrypts secret using the associated KMS key.
      # Depending on whether the secret is a string or binary, these needs to be handled differently
      if 'SecretString' in get_secret_value_response:
          secret = get_secret_value_response['SecretString']
      else:
          secret = base64.b64decode(get_secret_value_response['SecretBinary'])

      return secret   
