"""Module to scrape data from Bullma Broker."""
import os
from selenium.webdriver.common.by import By
from dotenv import load_dotenv
from bs4 import BeautifulSoup

load_dotenv()

class BullmaBroker:
    """
    Class to scrape data from Bull Market Broker.

    Attributes:
    - login_url: String with login url

    """
    def __init__(self):
        self.login_url = 'https://www.bullmarketbrokers.com/Security/SignIn'

    def get_portfolio(self, webdriver, raw_data_path, extract_date):
        """
        Get the portfolio data from Bull Market Broker.

        Args:
        - webdriver: WebDriver instance
        - raw_data_path: String with the path to save the raw data
        - extract_date: String with the date of the extraction

        Side effects:
        - Save the portfolio data in a html file
        """
        webdriver.get(self.login_url)

        username = os.getenv("USERNAME_BULLMA")
        password = os.getenv("PASSWORD_BULLMA")

        username_field = webdriver.find_element(By.ID, 'Email')
        password_field = webdriver.find_element(By.ID, 'Password')

        username_field.send_keys(username)
        password_field.send_keys(password)

        print("Se han llenado los campos de usuario y contraseña.")
        print("Por favor, inicie sesión manualmente y espera a que se carguen todos los recursos.")
        input("Posteriormente, presione Enter para continuar...")

        soup = BeautifulSoup(webdriver.page_source, 'html.parser')
        table_data = soup.find('table', {'class': 'fullWidth positionTable'})

        with open(
            f'{raw_data_path}\\bullma_portfolio_{extract_date}.html',
            'w', 
            encoding='utf-8'
        ) as f:
            f.write(table_data.prettify())
