"""
Browsers permitidos a utilizar en el argumento "browser":
- Google Chrome: "chrome"
- Mozilla Firefox: "mozilla"
"""

from brokers_web_scraping.web_scraping_interface import WebScrapingInterface
import argparse

if __name__ == "__main__":
    
    parser = argparse.ArgumentParser()
    parser.add_argument("--browser", type=str, help="Navegador a utilizar para el web scraping.")
    
    args = parser.parse_args()
    
    web_scraping_interface = WebScrapingInterface(**vars(args))
    web_scraping_interface()