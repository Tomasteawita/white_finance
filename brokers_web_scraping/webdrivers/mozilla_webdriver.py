from selenium.webdriver.firefox.options import Options
from selenium.webdriver.firefox.service import Service
from selenium import webdriver
from dotenv import load_dotenv

from os import getenv



class MozillaWebDriver():
    
    def __init__(self):
        
        load_dotenv()
        gecko_driver_path = getenv("GECKO_DRIVER_PATH")
        options = Options()
        options.headless = True
        service = Service(executable_path=gecko_driver_path)
        self.webdriver = webdriver.Firefox(service=service, options=options)
