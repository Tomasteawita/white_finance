""" IOL Broker Web Scraping Module """
import os
from bs4 import BeautifulSoup
from selenium.webdriver.common.by import By
from dotenv import load_dotenv

load_dotenv()

class IolBroker:
    """
    Class to scrape data from IOL Broker.

    Attributes:
    - login_url: String with login url
    - portfolio_url: String with portfolio url
    
    Methods:
    - get_portfolio: Get the portfolio data from IOL Broker
    """

    def __init__(self):
        self.login_url = 'https://micuenta.invertironline.com/ingresar'
        self.portfolio_url = 'https://iol.invertironline.com/MiCuenta/MiPortafolio'

    def get_portfolio(self, webdriver, BRONZE_DATA_PATH, extract_date):
        """
        Get the portfolio data from IOL Broker.

        Args:
        - webdriver: WebDriver instance
        - BRONZE_DATA_PATH: String with the path to save the raw data
        - extract_date: String with the date of the extraction

        Side effects:
        - Save the portfolio data in a html file
        """
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

        with open(
            f'{BRONZE_DATA_PATH}\\iol_portfolio_{extract_date}.html', 'w', encoding='utf-8'
        ) as f:
            f.write(table_data.prettify())
