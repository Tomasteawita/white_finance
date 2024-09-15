from bs4 import BeautifulSoup
from selenium.webdriver.common.by import By
from dotenv import load_dotenv
import os

load_dotenv()

class IolBroker:

    def __init__(self):
        self.login_url = 'https://micuenta.invertironline.com/ingresar'
        self.portfolio_url = 'https://iol.invertironline.com/MiCuenta/MiPortafolio'

    def get_portfolio(self, webdriver, raw_data_path, extract_date):
        webdriver.get(self.login_url)
        
        username = os.getenv("USERNAME_IOL")
        password = os.getenv("PASSWORD_IOL")
        
        username_field = webdriver.find_element(By.ID, 'usuario')
        password_field = webdriver.find_element(By.ID, 'password')

        username_field.send_keys(username)
        password_field.send_keys(password)
        
        print("Se han llenado los campos de usuario y contraseña.")
        print("Por favor, inicie sesión manualmente y espera a que se carguen todos los recursos.")
        input("Posteriormente, presione Enter para continuar...")

        webdriver.get(self.portfolio_url)

        soup = BeautifulSoup(webdriver.page_source, 'html.parser')
        table_data = soup.find('table', {'id': 'tablaactivos'})

        with open(f'{raw_data_path}\\iol_portfolio_{extract_date}.html', 'w') as f:
            f.write(table_data.prettify())
