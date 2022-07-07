import argparse
import asyncio
import json
import logging
import os
import sys

import uvicorn
from fastapi import FastAPI

sys.path.append('..')

from bfxapi import Client
from bfxapi import Order as BitfinexOrder

from trader_core.api_connection import AsyncApiConnection, PriceOffer
from trader_core.product_mapping import get_product_info

from bitfinx_order_book import AggregationOrderBook

class HedgingDealer():
   def __init__(self, configuration):
      self.hedging_settings = configuration['hedging_settings']

      # setup bitfinex connection
      self.bitfinex_book = AggregationOrderBook()

      self.bitfinex_config = configuration['bitfinex']

      self.bitfinex_balances = None
      self.bitfinex_orderbook_product = self.hedging_settings['bitfinex_orderbook_product']
      self.bitfinex_futures_hedging_product = self.hedging_settings['bitfinex_futures_hedging_product']
      self.bitfinex_leverage = self.hedging_settings['bitfinex_leverage']
      self.price_ratio = self.hedging_settings['price_ratio']
      self.leverex_product = self.hedging_settings['leverex_product']

      product_info = get_product_info(self.leverex_product)
      if product_info is None:
         raise Exception(f'Could not get info on product {self.leverex_product}')

      self._target_ccy_product = product_info.cash_ccy
      self._target_margin_product = product_info.margin_ccy
      self._target_crypto_ccy = product_info.crypto_ccy

      self.bitfinex_order_book_len = 100
      if 'bitfinex_order_book_len' in self.hedging_settings:
         self.bitfinex_order_book_len = self.hedging_settings['bitfinex_order_book_len']

      self.bitfinex_order_book_aggregation = 'P0'
      if 'bitfinex_order_book_aggregation' in self.hedging_settings:
         self.bitfinex_order_book_aggregation = self.hedging_settings['bitfinex_order_book_aggregation']

      bitfinex_log_level = 'INFO'
      if 'log_level' in self.bitfinex_config:
         bitfinex_log_level = self.bitfinex_config['log_level']

      self._bfx = Client(
        API_KEY=self.bitfinex_config['api_key'],
        API_SECRET=self.bitfinex_config['api_secret'],
        logLevel=bitfinex_log_level
      )

      self._bfx.ws.on('authenticated', self.on_bitfinex_authenticated)
      self._bfx.ws.on('balance_update', self.on_bitfinex_balance_updated)
      self._bfx.ws.on('wallet_snapshot', self.on_bitfinex_wallet_snapshot)
      self._bfx.ws.on('wallet_update', self.on_bitfinex_wallet_update)
      self._bfx.ws.on('order_book_update', self.on_bitfinex_order_book_update)
      self._bfx.ws.on('order_book_snapshot', self.on_bitfinex_order_book_snapshot)

      self._bfx.ws.on('order_new', self._on_bitfinex_order_new)
      self._bfx.ws.on('order_confirmed', self._on_bitfinex_order_confirmed)
      self._bfx.ws.on('order_closed', self._on_bitfinex_order_closed)
      self._bfx.ws.on('positions_new', self._on_bitfinex_positions_new)
      self._bfx.ws.on('positions_update', self._on_bitfinex_positions_update)
      self._bfx.ws.on('positions_close', self._on_bitfinex_positions_close)
      self._bfx.ws.on('margin_info_update', self._on_bitfinex_margin_info_update)

      # setup leverex connection
      # 'leverex' : ['api_endpoint', 'login_endpoint', 'key_file_path', 'email'],
      self._leverex_config = configuration['leverex']
      self._leverex_connection = AsyncApiConnection(customer_email=self._leverex_config['email'],
                                                    api_endpoint=self._leverex_config['api_endpoint'],
                                                    login_endpoint=self._leverex_config['login_endpoint'],
                                                    key_file_path=self._leverex_config['key_file_path'],)

      self.leverex_balances = {}
      self._bitfinex_balances = {}

      self._app = FastAPI()

      self._app.get('/')(self.report_api_entry)
      self._app.get('/api/balance')(self.report_balance)
      self._app.get('/api/leverex/session_info')(self.report_session_info)

      config = uvicorn.Config(self._app, port=configuration['status_server']['port'], log_level="info")
      self._status_server = uvicorn.Server(config)

      self._positions = {}
      self._net_exposure = 0.0
      self._current_session_info = None

   async def report_api_entry(self):
      return {
         '/api/balance' : 'current state balance',
         '/api/leverex/session_info' : 'info on current session on leverex'
         }

   async def report_balance(self):
      leverex_balances = {}
      leverex_balances['Buying power'] = '{} {}'.format(self.leverex_balances[self._target_ccy_product], self._target_ccy_product)
      leverex_balances['Margin'] = '{} {}'.format(self.leverex_balances[self._target_margin_product], self._target_margin_product)
      leverex_balances['Net exposure'] = '{} {}'.format(self._net_exposure, self._target_crypto_ccy)

      return { 'leverex' : leverex_balances, 'bitfinex' : self._bitfinex_balances}

   async def report_session_info(self):
      session_info = {}

      if self._current_session_info is not None:
         session_info['product'] = self._current_session_info.product_type
         session_info['id'] = self._current_session_info.session_id
         session_info['end time'] = str(self._current_session_info.cut_off_at)
         session_info['cutoff price'] = self._current_session_info.last_cut_off_price

      return session_info


   async def on_bitfinex_authenticated(self, auth_message):
      print('================= Authenticated to bitfinex')
      # subscribe to order book
      await self._bfx.ws.subscribe('book', self.bitfinex_orderbook_product,
                                  len=self.bitfinex_order_book_len,
                                  prec=self.bitfinex_order_book_aggregation)



   async def on_bitfinex_order_book_update(self, data):
      self.bitfinex_book.process_update(data['data'])
      await self.submit_offers()

   def on_bitfinex_order_book_snapshot(self, data):
      self.bitfinex_book.setup_from_snapshot(data['data'])

   def on_bitfinex_balance_updated(self, data):
      self._bitfinex_balances['USD total'] = float(data[0])
      self._bitfinex_balances['USD Net'] = float(data[1])

   def on_bitfinex_wallet_snapshot(self, wallets_snapshot):
      for wallet in wallets_snapshot:
         self.on_bitfinex_wallet_update(wallet)

   def on_bitfinex_wallet_update(self, wallet):
      if wallet.type not in self._bitfinex_balances:
         self._bitfinex_balances[wallet.type] = {}

      self._bitfinex_balances[wallet.type][wallet.currency] = wallet.balance

   async def _on_bitfinex_order_new(self, order):
      pass

   async def _on_bitfinex_order_confirmed(self, order):
      pass

   async def _on_bitfinex_order_closed(self, order):
      pass

   async def _on_bitfinex_positions_new(self, data):
      print(f'======= on_bitfinex_positions_new: {data}')

   async def _on_bitfinex_positions_update(self, data):
      print(f'======= on_bitfinex_positions_update: {data}')

   async def _on_bitfinex_positions_close(self, data):
      print(f'======= on_bitfinex_positions_close: {data}')

   async def _on_bitfinex_margin_info_update(self, data):
      print(f'======= on_bitfinex_margin_info_update: {data}')

   async def run(self):
      bitfinex_task = asyncio.create_task(self._bfx.ws.get_task_executable())
      leverex_task = asyncio.create_task(self._leverex_connection.run(self))

      await self._status_server.serve()

      # await asyncio.gather(self._status_server.serve(),
      #                      self._bfx.ws.get_task_executable(),
      #                      self._leverex_connection.run(self))

      # loop = asyncio.new_event_loop()
      # loop.run_until_complete(self._bfx.ws.get_task_executable())

      # asyncio.run(self._leverex_connection.run(service_url=self._leverex_config['api_endpoint']))

   async def updateOffer(self):
      print('===============  updateOffer =========')

   def get_ask_offer_volume(self):
      return 0.1

   def get_bid_offer_volume(self):
      return 0.01

   async def submit_offers(self):
      if len(self.leverex_balances) == 0:
         return

      ask_volume = self.get_ask_offer_volume()
      bid_volume = self.get_bid_offer_volume()

      ask = self.bitfinex_book.get_aggregated_ask_price(ask_volume)
      bid = self.bitfinex_book.get_aggregated_bid_price(bid_volume)

      if ask is not None and bid is not None:
         ask_price = ask.price*(1+self.price_ratio)
         bid_price = bid.price*(1-self.price_ratio)

         if ask_volume == bid_volume:
            offer = PriceOffer(volume=ask_volume, ask=ask_price, bid=bid_price)
            offers = [offer]
         else:
            ask_offer = PriceOffer(volume=ask_volume, ask=ask_price)
            bid_offer = PriceOffer(volume=bid_volume, bid=bid_price)

            offers = [ask_offer, bid_offer]

         await self._leverex_connection.submit_offers(target_product=self.leverex_product, offers=offers)
      else:
         print('Book is not loaded')

   def on_connected(self):
      pass

   async def on_authorized(self):
      await self._leverex_connection.load_open_positions(target_product=self.leverex_product, callback=self.on_positions_loaded)
      await self._leverex_connection.subscribe_session_open(self.leverex_product)

   def on_market_data(self, update):
      print('on_market_data: {}'.format(update))

   def onLoadBalance(self, balances):
      print('Balance loaded {}'.format(balances))
      for balance_info in balances:
         self.leverex_balances[balance_info['currency']] = float(balance_info['balance'])

   def onSubmitPrices(self, update):
      pass

   def on_session_open(self, update):
      if update.product_type == self.leverex_product:
         self._current_session_info = update

   def on_session_closed(self, update):
      if update.product_type == self.leverex_product:
         self._current_session_info = None

   async def on_positions_loaded(self, orders):
      print(f'======== {len(orders)} posiions loaded')
      for order in orders:
         self.store_active_order(order)

   async def _create_bitfinex_order(self, leverex_order):
      price = leverex_order.price
      # negative amount is sell, positive is buy
      # we need to invert leverex side here
      if leverex_order.is_sell:
         quantity = leverex_order.quantity
      else:
         quantity = -leverex_order.quantity

      logging.info(f'Submitting order to bitfinex {quantity}@{price}')

      await self._bfx.ws.submit_order(symbol=self.bitfinex_futures_hedging_product,
                                      leverage=self.bitfinex_leverage,
                                      price=price,
                                      amount=quantity,
                                      market_type=BitfinexOrder.Type.MARKET)

   def store_active_order(self, order):
      self._positions[order.id] = order
      if order.is_sell:
         self._net_exposure = self._net_exposure - order.quantity
      else:
         self._net_exposure = self._net_exposure + order.quantity

   # position matched on leverex
   def on_order_created(self, order):
      if order.product_type == self.leverex_product:
         if order.is_trade_position:
            # create order on bitfinex
            asyncio.create_task(self._create_bitfinex_order(order))
         self.store_active_order(order)

   def on_order_filled(self, order):
      if order.product_type == self.leverex_product:
         if order.id in self._positions:
            self._positions.pop(order.id)
            if order.is_sell:
               self._net_exposure = self._net_exposure + order.quantity
            else:
               self._net_exposure = self._net_exposure - order.quantity
         else:
            logging.error(f'[on_order_filled] order {order.id} is not in a list')

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
      'status_server' : ['port'],
      'leverex' : ['api_endpoint', 'login_endpoint', 'key_file_path', 'email'],
      'bitfinex' : ['api_key', 'api_secret'],
      'hedging_settings' : ['leverex_product',
                            'bitfinex_futures_hedging_product',
                            'bitfinex_orderbook_product',
                            'price_ratio',
                            'bitfinex_leverage']
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
   logging.getLogger("asyncio").setLevel(logging.DEBUG)

   main(configuration=configuration)

   exit(0)
