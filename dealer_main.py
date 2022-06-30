import logging
import asyncio
import json
import os
import websockets.exceptions
import random
import argparse

from trader_core.api_connection import AsyncApiConnection
from trader_core.market_event_listener import MarketEventListener
from trader_core.login_connection import LoginServiceClientWS

################################################################################
class SampleDealer(MarketEventListener):
   threshold_volume = 0.01

   def __init__(self):
      super().__init__()
      self.spread_per_btc = 60 + random.randrange(100)
      self.counter = 0

   def on_market_data(self, data):
      self.counter += 1
      if self.counter % 5 != 0:
         return

      #return bogus price on each index price update
      index_price = float(data['market_data']['live_cutoff'])
      if index_price == 0:
         return

      if not self.balance_awaitable:
         return

      #figure out volume to quote based on free cash
      #index_price will be used as an approximation of session open price
      session_im = index_price / 10
      max_vol = (self.freeCash / session_im) * 0.9
      offers = []

      #create offers based on spread per volume, halve volume of successive
      #offers until thresold_volume is reached
      current_vol = max_vol
      while current_vol > self.threshold_volume:
         offers.append(
            {
               'volume' : str(current_vol),
               'ask'    : str(index_price + self.spread_per_btc * current_vol),
               'bid'    : str(index_price - self.spread_per_btc * current_vol)
            })

         current_vol = current_vol / 2
         if len(offers) >= 5:
            break

      #send the offers
      self.sendOffer(offers)

   async def updateOffer(self):
      await asyncio.sleep(60)
      pass

################################################################################
class TestDealer(MarketEventListener):
   def __init__(self):
      super().__init__()
      self.cutoff_price = 0

   def on_market_data(self, data):
      #push an offer every minute, for TTL testing purposes

      self.cutoff_price = float(data['market_data']['live_cutoff'])
      return

   async def updateOffer(self):
      while True:
         if self.cutoff_price == 0:
            await asyncio.sleep(1)

         ask = self.cutoff_price + 500
         bid = self.cutoff_price - 500

         tight_ask = self.cutoff_price + 30 + random.randrange(10)
         tight_bid = self.cutoff_price - 30 - random.randrange(10)

         self.sendOffer([
               {
                  'volume' : '0.05',
                  'ask'    : str(tight_ask),
                  'bid'    : str(tight_bid)
               },
               {
                  'volume' : '0.2',
                  'ask'    : str(ask),
                  'bid'    : str(bid)
               }
               ])

         await asyncio.sleep(45)

################################################################################
if __name__ == '__main__':
   LOG_FORMAT = (
      "%(asctime)s,%(msecs)d %(levelname)-8s [%(filename)s:%(lineno)d] %(message)s"
   )
   logging.basicConfig(level=logging.INFO, format=LOG_FORMAT)

   input_parser = argparse.ArgumentParser()
   input_parser.add_argument('--datadir',
      help='path to running folder',
      required=True,
      action='store')

   args = input_parser.parse_args()
   home = args.datadir

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
