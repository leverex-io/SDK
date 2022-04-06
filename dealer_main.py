import logging
import asyncio
import json
import os
import websockets.exceptions
import random

from trader_core.api_connection import AsyncApiConnection
from trader_core.market_event_listener import MarketEventListener
from trader_core.login_connection import LoginServiceClientWS

home = "uat/dealer_usd"

################################################################################
class SampleDealer(MarketEventListener):
   def __init__(self):
      super().__init__()

   def onMarketData(self, data):
      #return bogus price on each cutoff price update

      cutoff_price = float(data['market_data']['live_cutoff'])
      if cutoff_price == 0:
         return

      if not self.balance_awaitable:
         return

      ask = cutoff_price + 50
      bid = cutoff_price - 50

      tight_ask = cutoff_price + 30 + random.randrange(10)
      tight_bid = cutoff_price - 30 - random.randrange(10)

      # print(f'Sending price update: {bid} : {ask}')

      submit_prices_request = {
         'submit_prices' : {
            'product_type' : self.target_product,
            'prices' : [
            {
               'volume' : '0.1',
               'ask'    : str(tight_ask),
               'bid'    : str(tight_bid)
            },
            {
               'volume' : '1',
               'ask'    : str(ask),
               'bid'    : str(bid)
            }
            ]
         }
      }

      self.send(submit_prices_request)

################################################################################
if __name__ == '__main__':
   LOG_FORMAT = (
      "%(asctime)s,%(msecs)d %(levelname)-8s [%(filename)s:%(lineno)d] %(message)s"
   )
   logging.basicConfig(level=logging.INFO, format=LOG_FORMAT)

   # load settings
   with open('{}/config.json'.format(home)) as json_file:
      settings = json.load(json_file)

   auth_type_string = None

   api_endpoint = os.environ.get('API_ENDPOINT')
   if api_endpoint is None or len(api_endpoint) == 0:
      if 'api_endpoint' not in settings:
         logging.error('API_ENDPOINT not set')
         exit(1)
      api_endpoint = settings['api_endpoint']

   login_endpoint = os.environ.get('LOGIN_SERVICE_ENDPOINT')
   if login_endpoint is None or len(login_endpoint) == 0:
      if 'login_endpoint' not in settings:
         logging.error('LOGIN_SERVICE_ENDPOINT not set')
         exit(1)
      login_endpoint = settings['login_endpoint']

   keyPath = '{}/key.pem'.format(home)

   if 'email' not in settings:
      logging.error('missing email')
      exit(1)

   dealer_email = settings['email']
   auth_type_string = f'Email: {dealer_email}'
   login_client = LoginServiceClientWS(email=dealer_email,\
                                       login_endpoint=login_endpoint,\
                                       private_key_path=keyPath,\
                                       dump_communication=False)

   logging.info('API api endpoint: {}'.format(api_endpoint))
   logging.info('Login service endpoint: {}'.format(login_endpoint))
   logging.info(auth_type_string)

   dealer = SampleDealer()
   connection = AsyncApiConnection(login_client, dealer)

   while True:
      try:
         asyncio.run(connection.run(service_url=api_endpoint))
         logging.warning("event loop ended gracefully, exiting")
         exit(0)
      except websockets.exceptions.ConnectionClosedError as e:
         logging.warning("connection was closed, restarting")
