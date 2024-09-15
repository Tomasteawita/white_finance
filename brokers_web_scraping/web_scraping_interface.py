from importlib import import_module
from os import getenv
from datetime import datetime
class WebScrapingInterface:
    
    def __init__(self, **kwargs):
        driver_module = import_module(f'.webdrivers.{kwargs.get("browser")}_webdriver', package='brokers_web_scraping')
        driver_class = getattr(driver_module, f'{kwargs.get("browser").capitalize()}WebDriver')
        self.webdriver = driver_class().webdriver
        self.raw_data_path = getenv("RAW_DATA_PATH")
        self.broker_modules_dict = {
            "Bull Market": "bullma",
            "IOL": "iol"
        }
        
    
    def _set_broker(self):
        """
        Setea el broker que se desea utilizar.
        
        Args:
        - broker: str
        
        Returns:
        - broker_instance: Broker instance
        """
        while True:
            broker = input("""
            ¿De qué broker deseas obtener información? (Escriba el nombre del broker)
                        1. Bull Market
                        2. IOL
            """)
            print(f"Obteniendo información de {broker}...")

            broker_module_name = self.broker_modules_dict.get(broker)

            if broker_module_name is None:
                print("Broker no válido. Por favor, ingrese un broker válido.")
                continue
            else:
                broker_module = import_module(f'brokers_web_scraping.brokers.{broker_module_name}')
                broker_class = getattr(broker_module, f'{self.broker_modules_dict[broker].capitalize()}Broker')
                print("Broker seteado.")
            return broker_class()
    
    def __call__(self):

        broker_instance = self._set_broker()

        while True:
            action = input("""
            ¿Qué acción deseas realizar? (Escoge el número de la acción)
                        1. Obtener información de la cartera
                        2. Salir
            """)
            match action:
                case "1":
                    # obtengo el dia de hoy en formato YYYY-MM-DD
                    extract_date = datetime.now().strftime("%Y-%m-%d")
                    broker_instance.get_portfolio(self.webdriver, self.raw_data_path, extract_date)
                case "2":
                    break
                case _:
                    print("Acción no válida. Por favor, ingrese una acción válida.")
        
        self.webdriver.quit()