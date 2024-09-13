from bs4 import BeautifulSoup
from selenium.webdriver.common.by import By
from dotenv import load_dotenv
from importlib import import_module
from datetime import datetime
import os

load_dotenv()

class BullmaBroker:
    
    def __init__(self, **kwargs) -> None:
        driver_module = import_module(f'.webdrivers.{kwargs.get("browser")}_webdriver', package='brokers_web_scraping')
        driver_class = getattr(driver_module, f'{kwargs.get("browser").capitalize()}WebDriver')
        self.webdriver = driver_class().webdriver
        self.raw_data_path = os.getenv("RAW_DATA_PATH")
    
    def get_portfolio(self):
        driver = self.webdriver
        driver.get('https://www.bullmarketbrokers.com/Security/SignIn')
        
        username = os.getenv("USERNAME_BULLMA")
        password = os.getenv("PASSWORD_BULLMA")
        
        username_field = driver.find_element(By.ID, 'Email')
        password_field = driver.find_element(By.ID, 'Password')

        username_field.send_keys(username)
        password_field.send_keys(password)
        
        # Obtengo dia de la fecha en formato YYYY-MM-DD
        date = datetime.now().strftime('%Y-%m-%d')
        
        
        print("Se han llenado los campos de usuario y contraseña.")
        print("Por favor, inicie sesión manualmente y espera a que se carguen todos los recursos.")
        input("Posteriormente, presione Enter para continuar...")
        

        soup = BeautifulSoup(driver.page_source, 'html.parser')
        table_data = soup.find('table', {'class': 'fullWidth positionTable'})

        with open(f'{self.raw_data_path}\\bullma_portfolio_{date}.html', 'w') as f:
            f.write(table_data.prettify())

        driver.quit()