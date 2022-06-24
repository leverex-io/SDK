import argparse
import asyncio
import json
import logging
import os
import sys

sys.path.append('..')

from bfxapi import Client

from trader_core.login_connection import LoginServiceClientWS
from trader_core.api_connection import AsyncApiConnection

class HedgingDealer():
   def __init__(self, configuration):
      # setup bitfinex connection
      bitfinex_log_level = 'INFO'
      self.bitfinex_config = configuration['bitfinex']
      self.id = 123

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

      # setup leverex connection
      # 'leverex' : ['api_endpoint', 'login_endpoint', 'key_file_path', 'email'],
      self.leverex_config = configuration['leverex']
      self.login_client = LoginServiceClientWS(email=self.leverex_config['email'],
                                       login_endpoint=self.leverex_config['login_endpoint'],
                                       private_key_path=self.leverex_config['key_file_path'],
                                       dump_communication=True)
      self.leverex_connection = AsyncApiConnection(self.login_client, self)

      self.target_product = 'xbtusd_rf'

   def on_bitfinex_authenticated(self, auth_message):
      print('================= Authenticated to bitfinex: {} {}'.format(self.id, auth_message))

   def on_bitfinex_balance_updated(self, data):
      print(f'======= on_bitfinex_balance_updated: {data}')

   def on_bitfinex_wallet_snapshot(self, data):
      print(f'======= on_bitfinex_wallet_snapshot: {data}')

   def on_bitfinex_wallet_update(self, data):
      print(f'======= on_bitfinex_wallet_update: {data}')

   async def run(self):
      await asyncio.gather(self.bfx.ws.get_task_executable(),
                           self.leverex_connection.run(service_url=self.leverex_config['api_endpoint']))

      # loop = asyncio.new_event_loop()
      # loop.run_until_complete(self.bfx.ws.get_task_executable())

      # asyncio.run(self.leverex_connection.run(service_url=self.leverex_config['api_endpoint']))

   async def updateOffer(self):
      pass

   def onMarketData(self, update):
      pass

   def onLoadBalanceInner(self, update):
      print('Balance loaded {}'.format(update))

   def onSubmitPrices(self, update):
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
