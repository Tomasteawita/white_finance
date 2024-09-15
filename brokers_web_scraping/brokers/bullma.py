from bs4 import BeautifulSoup
from selenium.webdriver.common.by import By
from dotenv import load_dotenv
import os

load_dotenv()

class BullmaBroker:
    
    def __init__(self):
        self.login_url = 'https://www.bullmarketbrokers.com/Security/SignIn'
    
    def get_portfolio(self, webdriver, raw_data_path, extract_date):
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

        with open(f'{raw_data_path}\\bullma_portfolio_{extract_date}.html', 'w') as f:
            f.write(table_data.prettify())