import argparse
import asyncio
import json
import logging
import os
import sys

sys.path.append('..')

from bfxapi import Client

from trader_core.login_connection import LoginServiceClientWS
from trader_core.api_connection import AsyncApiConnection, PriceOffer

from bitfinx_order_book import AggregationOrderBook

class HedgingDealer():
   def __init__(self, configuration):
      self.hedging_settings = configuration['hedging_settings']

      # setup bitfinex connection
      self.bitfinex_book = AggregationOrderBook()

      self.bitfinex_config = configuration['bitfinex']

      self.bitfinex_balances = None
      self.bitfinex_futures_hedging_product = self.hedging_settings['bitfinex_futures_hedging_product']
      self.fixed_volume = self.hedging_settings['fixed_volume']
      self.price_ratio = self.hedging_settings['price_ratio']
      self.leverex_product = self.hedging_settings['leverex_product']

      self.bitfinex_order_book_len = 100
      if 'bitfinex_order_book_len' in self.hedging_settings:
         self.bitfinex_order_book_len = self.hedging_settings['bitfinex_order_book_len']

      self.bitfinex_order_book_aggregation = 'P0'
      if 'bitfinex_order_book_aggregation' in self.hedging_settings:
         self.bitfinex_order_book_aggregation = self.hedging_settings['bitfinex_order_book_aggregation']

      bitfinex_log_level = 'INFO'
      if 'log_level' in self.bitfinex_config:
         bitfinex_log_level = self.bitfinex_config['log_level']

      self.bfx = Client(
        API_KEY=self.bitfinex_config['api_key'],
        API_SECRET=self.bitfinex_config['api_secret'],
        logLevel=bitfinex_log_level
      )

      self.bfx.ws.on('authenticated', self.on_bitfinex_authenticated)
      self.bfx.ws.on('balance_update', self.on_bitfinex_balance_updated)
      self.bfx.ws.on('wallet_snapshot', self.on_bitfinex_wallet_snapshot)
      self.bfx.ws.on('wallet_update', self.on_bitfinex_wallet_update)
      self.bfx.ws.on('order_book_update', self.on_bitfinex_order_book_update)
      self.bfx.ws.on('order_book_snapshot', self.on_bitfinex_order_book_snapshot)

      # setup leverex connection
      # 'leverex' : ['api_endpoint', 'login_endpoint', 'key_file_path', 'email'],
      self.leverex_config = configuration['leverex']
      self.login_client = LoginServiceClientWS(email=self.leverex_config['email'],
                                       login_endpoint=self.leverex_config['login_endpoint'],
                                       private_key_path=self.leverex_config['key_file_path'],
                                       dump_communication=True)
      self.leverex_connection = AsyncApiConnection(self.login_client, self)

      self.leverex_balances = {}


   async def on_bitfinex_authenticated(self, auth_message):
      print('================= Authenticated to bitfinex')
      # subscribe to order book
      await self.bfx.ws.subscribe('book', self.bitfinex_futures_hedging_product,
                                  len=self.bitfinex_order_book_len,
                                  prec=self.bitfinex_order_book_aggregation)

   def on_bitfinex_order_book_update(self, data):
      self.bitfinex_book.process_update(data['data'])
      self.submit_offers()

   def on_bitfinex_order_book_snapshot(self, data):
      self.bitfinex_book.setup_from_snapshot(data['data'])

   def on_bitfinex_balance_updated(self, data):
      print(f'======= on_bitfinex_balance_updated: {data}')

   def on_bitfinex_wallet_snapshot(self, data):
      print(f'======= on_bitfinex_wallet_snapshot: {data}')
      for wallet in data:
         print(f'======= on_bitfinex_wallet_snapshot: {wallet}')

   def on_bitfinex_wallet_update(self, data):
      print(f'======= on_bitfinex_wallet_update: {data}')

   async def run(self):
      await asyncio.gather(self.bfx.ws.get_task_executable(),
                           self.leverex_connection.run(service_url=self.leverex_config['api_endpoint']))

      # loop = asyncio.new_event_loop()
      # loop.run_until_complete(self.bfx.ws.get_task_executable())

      # asyncio.run(self.leverex_connection.run(service_url=self.leverex_config['api_endpoint']))

   async def updateOffer(self):
      print('===============  updateOffer')

   def submit_offers(self):
      if len(self.leverex_balances) == 0:
         return
      ask = self.bitfinex_book.get_aggregated_ask_price(self.fixed_volume)
      bid = self.bitfinex_book.get_aggregated_bid_price(self.fixed_volume)

      if ask is not None and bid is not None:
         print('Submitting offer')
         offer = PriceOffer(volume=self.fixed_volume, ask=ask.price, bid=bid.price)
         self.leverex_connection.submit_offers(target_product=self.leverex_product, offers=[offer])
      else:
         print('Book is not loaded')

   def on_authorized(self):
      print('======= Authorized to leverex')
      self.submit_offers()

   def onMarketData(self, update):
      print('onMarketData: {}'.format(update))
      pass

   def onLoadBalance(self, balances):
      print('Balance loaded {}'.format(balances))
      for balance_info in balances:
         self.leverex_balances[balance_info['currency']] = float(balance_info['balance'])

   def onSubmitPrices(self, update):
      print(f'======= onSubmitPrices: {update}')
      pass

   def onOrderUpdateInner(self, update):
      pass

def main(configuration):
   dealer = HedgingDealer(configuration=configuration)
   asyncio.run(dealer.run())

if __name__ == '__main__':
   input_parser = argparse.ArgumentParser()

   input_parser.add_argument('--config_file',
                             help='Set config file in JSON format',
                             action='store',
                             required=True)

   args = input_parser.parse_args()

   required_settings = {
      'leverex' : ['api_endpoint', 'login_endpoint', 'key_file_path', 'email'],
      'bitfinex' : ['api_key', 'api_secret'],
      'hedging_settings' : ['leverex_product', 'bitfinex_futures_hedging_product', 'fixed_volume', 'price_ratio']
   }

   with open(args.config_file) as json_config_file:
      configuration = json.load(json_config_file)

   # validate configs
   for k in required_settings:
      if k not in configuration:
         logging.error(f'Missing {k} in config')
         exit(1)

      for kk in required_settings[k]:
         if kk not in configuration[k]:
            logging.error(f'Missing {kk} in config group {k}')
            exit(1)

   log_level = 'INFO'

   if 'log_level' in configuration:
      log_level = configuration['log_level']

   logging.basicConfig(level=log_level)

   main(configuration=configuration)

   exit(0)
