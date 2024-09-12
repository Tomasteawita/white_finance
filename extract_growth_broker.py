import requests
from bs4 import BeautifulSoup
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.firefox.options import Options
from selenium.webdriver.firefox.service import Service
from dotenv import load_dotenv
import os

load_dotenv()

# Specify the path to your GeckoDriver
gecko_driver_path = "./geckodriver.exe"

# Create options for Firefox
options = Options()
options.headless = True

# Create a Service object for GeckoDriver
service = Service(executable_path=gecko_driver_path)

# Create the WebDriver object using the service
driver = webdriver.Firefox(service=service, options=options)

# 2. Navegar a la página de inicio de sesión
driver.get('https://www.bullmarketbrokers.com/Security/SignIn')

username = os.getenv("USERNAME_BROKER")
password = os.getenv("PASSWORD_BROKER")

# 3. Completar el formulario de login
username_field = driver.find_element(By.ID, 'Email')
password_field = driver.find_element(By.ID, 'Password')
username_field.send_keys(username)
password_field.send_keys(password)

# 4. Enviar el formulario
# login_button = driver.find_element(By.ID, 'submitButton')
# login_button.submit()

# espero 30 segundos a que inicie sesión manualmente
input("Presione Enter para continuar...")


# Al ejeuctar el submit, la página se redirige a la página del dashboard

# 6. Esperar a que la tabla se cargue (si es necesario)
try:
    table = WebDriverWait(driver, 10).until(
        # EC.presence_of_element_located((By.ID, 'id_de_la_tabla'))
        # Obtengo la tabla a partir de la clase
        EC.presence_of_element_located((By.CLASS_NAME, 'logger-pill pull-left fullWidth'))
    )
except Exception as e:
    print("La interfaz no se cargó en el tiempo establecido.")

# # 7. Extraer los datos de la tabla
soup = BeautifulSoup(driver.page_source, 'html.parser')
# table_data = soup.find('table', {'id': 'id_de_la_tabla'})
table_data = soup.find('table', {'class': 'fullWidth positionTable'})

# # 8. Imprimir los datos de la tabla
print(table_data.prettify())

# 9 Dejo los datos de la tabla en un archivo
with open('table_data.html', 'w') as f:
    f.write(table_data.prettify())

# 10. Cerrar el navegador
driver.quit()