import pyRofex
from dotenv import load_dotenv
import os

load_dotenv()

# Set the the parameter for the REMARKET environment
murl = "https://api.bull.xoms.com.ar/"
mwss = "wss://api.bull.xoms.com.ar/"

pyRofex._set_environment_parameter("url", murl, pyRofex.Environment.LIVE)
pyRofex._set_environment_parameter("ws", mwss, pyRofex.Environment.LIVE)
pyRofex._set_environment_parameter("proprietary", "https://matriz.bull.xoms.com.ar/", pyRofex.Environment.LIVE)

username = os.getenv("USERNAME_MATRIZ")
password = os.getenv("PASSWORD_MATRIZ")
account = os.getenv("ACCOUNT_MARKET")


pyRofex.initialize(username,
                   password,
                   account,
                   pyRofex.Environment.LIVE)

print(pyRofex.get_market_data("MERV - XMEV - GGAL - 24hs"))
