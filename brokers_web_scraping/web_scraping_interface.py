from importlib import import_module

class WebScrapingInterface:
    
    def __init__(self, **kwargs):
        self.browser = kwargs.get("browser")
        self.broker = kwargs.get("broker")
        self.action = kwargs.get("action")
    
    def __call__(self):
        broker_module = import_module(f'brokers_web_scraping.brokers.{self.broker}')
        broker_class = getattr(broker_module, f'{self.broker.capitalize()}Broker')
        broker_kwargs = {"browser": self.browser}
        broker_instance = broker_class(**broker_kwargs)
        
        if self.action == "get_portfolio":
            broker_instance.get_portfolio()