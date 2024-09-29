from selenium import webdriver
from dotenv import load_dotenv


class ChromeWebDriver:
    
    def __init__(self) -> None:
        load_dotenv()
        self.webdriver = webdriver.Chrome()
    
    