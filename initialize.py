import pyRofex

# Set the the parameter for the REMARKET environment
murl = "https://api.bull.xoms.com.ar/"
mwss = "wss://api.bull.xoms.com.ar/"

pyRofex._set_environment_parameter("url", murl, pyRofex.Environment.LIVE)
pyRofex._set_environment_parameter("ws", mwss, pyRofex.Environment.LIVE)
pyRofex._set_environment_parameter("proprietary", "https://matriz.bull.xoms.com.ar/", pyRofex.Environment.LIVE)


pyRofex.initialize("username",
                   "password",
                   "account",
                   pyRofex.Environment.LIVE)

print(pyRofex.get_market_data("MERV - XMEV - AL30D - 24hs"))


# In case you have a previously generated and active token, you will be able to do the following
# pyRofex.initialize(user="sampleUser",
#                    password="samplePassword",
#                    account="sampleAccount",
#                    environment=pyRofex.Environment.REMARKET,
#                    active_token="activeToken")