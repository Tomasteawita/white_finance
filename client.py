from brokers_web_scraping.web_scraping_interface import WebScrapingInterface
import argparse

if __name__ == "__main__":
    
    parser = argparse.ArgumentParser()
    parser.add_argument("--browser", type=str, help="Navegador a utilizar para el web scraping.")
    parser.add_argument("--broker", type=str, help="Broker a utilizar para el web scraping, ejemplo: bullma, iol, balanz, etc.")
    parser.add_argument("--action", type=str, help="Acción a realizar en el broker.")
    
    args = parser.parse_args()
    
    web_scraping_interface = WebScrapingInterface(**vars(args))
    web_scraping_interface()