import argparse
import asyncio
import json
import logging
import os
import sys

import uvicorn
from fastapi import FastAPI
from fastapi.responses import HTMLResponse

sys.path.append('..')

from bfxapi import Client
from bfxapi import Order as BitfinexOrder
from bfxapi.models import Position
from bfxapi.models import Notification

from trader_core.api_connection import AsyncApiConnection, PriceOffer
from trader_core.product_mapping import get_product_info

from bitfinx_order_book import AggregationOrderBook

class DepositWithdrawAddresses():
   def __init__(self, market_name):
      self._market_name = market_name
      self._deposit_address = None
      self._withdraw_address = None

   def set_withdraw_addresses(self, addresses):
      self._withdraw_address = addresses

   def get_withdraw_addresses(self):
      return self._withdraw_address

   def set_deposit_address(self, address):
      self._deposit_address = address

   def get_deposit_address(self):
      return self._deposit_address

class HedgingDealer():
   def __init__(self, configuration):
      self.hedging_settings = configuration['hedging_settings']

      self._overseer_mode = False
      if 'overseer_mode' in self.hedging_settings:
         self._overseer_mode = bool(self.hedging_settings['overseer_mode'])

      # setup bitfinex connection
      self.bitfinex_book = AggregationOrderBook()

      self.bitfinex_config = configuration['bitfinex']

      self.bitfinex_balances = None
      self.bitfinex_orderbook_product = self.hedging_settings['bitfinex_orderbook_product']
      self.bitfinex_margin_wallet = self.hedging_settings['bitfinex_margin_wallet']
      self.bitfinex_futures_hedging_product = self.hedging_settings['bitfinex_futures_hedging_product']
      self.min_bitfinex_leverage = self.hedging_settings['min_bitfinex_leverage']
      self.bitfinex_leverage = self.hedging_settings['bitfinex_leverage']
      self.max_bitfinex_leverage = self.hedging_settings['max_bitfinex_leverage']
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
      self._bfx.ws.on('position_snapshot', self._on_bitfinex_position_snapshot)
      self._bfx.ws.on('position_new', self._on_bitfinex_position_new)
      self._bfx.ws.on('position_update', self._on_bitfinex_position_update)
      self._bfx.ws.on('position_close', self._on_bitfinex_position_close)
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
      self._app.get('/api/rebalance_state')(self.report_rebalance_state)
      self._app.get('/api/leverex/session_info')(self.report_session_info)
      self._app.get('/api/bitfinex/position')(self.report_bitfinex_position)

      config = uvicorn.Config(self._app, host='0.0.0.0', port=configuration['status_server']['port'], log_level="debug")
      self._status_server = uvicorn.Server(config)

      self._positions = {}
      self._net_exposure = 0.0
      self._current_session_info = None

      self._bitfinex_position_loaded = False
      self._leverex_orders_loaded = False

      self._bitfinex_positions = { self.bitfinex_futures_hedging_product : None }

      self._bitfinex_deposit_addresses = None
      self._leverex_deposit_addresses = None
      self._rebalance_enabled = False
      self._rebalance_disable_reason = "Address info not loaded"

   def _validate_rebalance_feature_state(self):
      if self._leverex_deposit_addresses is None:
         self._rebalance_enabled = False
         self._rebalance_disable_reason = "Leverex addresses info not loaded"
         return

      if self._leverex_deposit_addresses.get_withdraw_addresses() is None:
         self._rebalance_enabled = False
         self._rebalance_disable_reason = "Leverex whitelisted addresses info not loaded"
         return

      if self._leverex_deposit_addresses.get_deposit_address() is None:
         self._rebalance_enabled = False
         self._rebalance_disable_reason = "Leverex deposit addresses info not loaded"
         return

      if len(self._leverex_deposit_addresses.get_deposit_address()):
         self._rebalance_enabled = False
         self._rebalance_disable_reason = "Leverex deposit addresses is empty"
         return

      if self._bitfinex_deposit_addresses is None:
         self._rebalance_enabled = False
         self._rebalance_disable_reason = "Bitfinex addresses info not loaded"
         return

      if self._bitfinex_deposit_addresses.get_deposit_address() is None:
         self._rebalance_enabled = False
         self._rebalance_disable_reason = "Bitfinex deposit addresses info not loaded"
         return

      if len(self._bitfinex_deposit_addresses.get_deposit_address()):
         self._rebalance_enabled = False
         self._rebalance_disable_reason = "Bitfinex deposit addresses is empty"
         return

      # validate that Bitfinex address is whitelisted on leverex
      if self._bitfinex_deposit_addresses.get_deposit_address() not in self._leverex_deposit_addresses.get_withdraw_addresses():
         self._rebalance_enabled = False
         self._rebalance_disable_reason = "Bitfinex deposit addresses is not whitelisted on leverex"
         return

      # ATM we do not load whitelisted addresses for Bitfinex, since it is done via
      # different API endpoint and undocumented API request

      self._rebalance_enabled = True

   async def report_api_entry(self):
      html_content = """
       <html>
           <head>
               <title>Delaer API</title>
           </head>
           <body>
               <p><a href="/api/balance">current state balance</a></p>
               <p><a href="/api/leverex/session_info">info on current session on leverex</a></p>
               <p><a href="/api/bitfinex/position">info on current position on bitfinex</a></p>
               <p><a href="/api/rebalance_state">Info on rebalance related data from both platforms</a></p>
           </body>
       </html>
       """
      return HTMLResponse(content=html_content, status_code=200)

   async def report_rebalance_state(self):
      result = {}

      if self._bitfinex_deposit_addresses is not None:
         info = {}

         withdraw_addresses = self._bitfinex_deposit_addresses.get_withdraw_addresses()

         info['deposit address'] = self._bitfinex_deposit_addresses.get_deposit_address()
         info['withdraw addresses'] = withdraw_addresses if withdraw_addresses is not None else 'Loading not supported'

         result['bitfinex'] = info
      else:
         result['bitfinex'] = 'Not loaded'

      if self._leverex_deposit_addresses is not None:
         info = {}

         withdraw_addresses = self._leverex_deposit_addresses.get_withdraw_addresses()

         info['deposit address'] = self._leverex_deposit_addresses.get_deposit_address()
         info['withdraw addresses'] = withdraw_addresses if withdraw_addresses is not None else 'Not loaded'

         result['leverex'] = info
      else:
         result['leverex'] = 'Not loaded'

      if self._rebalance_enabled:
         result['rebalance state'] = 'Enabled'
      else:
         result['rebalance state'] = f'Disabled: {self._rebalance_disable_reason}'

      return result

   async def report_balance(self):
      leverex_balances = {}

      portfolio = None
      leverex_total = None
      bitfinex_total = None

      if len(self.leverex_balances) != 0:
         leverex_balances['Buying power'] = '{} {}'.format(self.leverex_balances.get(self._target_ccy_product, 'Not loaded'), self._target_ccy_product)
         leverex_balances['Margin'] = '{} {}'.format(self.leverex_balances.get(self._target_margin_product, 'Not loaded'), self._target_margin_product)
         leverex_balances['Net exposure'] = '{} {}'.format(self._net_exposure, self._target_crypto_ccy)

         leverex_total = self._get_buying_power() + self._get_margin_reserved()
         leverex_balances['total'] = leverex_total

      bitfinex_balances = self._bitfinex_balances.copy()

      position_info = self._bitfinex_positions[self.bitfinex_futures_hedging_product]
      if position_info is None:
         bitfinex_balances['position'] = 'Closed'
      else:
         bitfinex_balances['position'] = position_info.amount

      if 'margin' in self._bitfinex_balances:
         if self.bitfinex_margin_wallet in self._bitfinex_balances['margin']:
            bitfinex_total = float(self._bitfinex_balances['margin'][self.bitfinex_margin_wallet]['total'])

      if bitfinex_total is not None and leverex_total is not None:
         portfolio = bitfinex_total + leverex_total

      return { 'leverex' : leverex_balances, 'bitfinex' : bitfinex_balances, 'portfolio' : portfolio}

   async def report_bitfinex_position(self):
      response = {}

      for info in self._bitfinex_positions.items():
         product = info[0]
         position = info[1]

         if position is None:
            response[product] = 'Closed'
         else:
            position_info = {}

            position_info['amount'] = position.amount
            position_info['base price'] = position.base_price
            position_info['liquidation price'] = position.liquidation_price
            position_info['leverage'] = position.leverage
            position_info['type'] = 'Derivatives position' if position.type == 1 else 'Margin position'
            position_info['Collateral'] = position.collateral
            position_info['Collateral MIN'] = position.collateral_min

            response[position.symbol] = position_info

      return response

   async def report_session_info(self):
      session_info = {}

      if self._current_session_info is not None:
         session_info['product'] = self._current_session_info.product_type
         session_info['id'] = self._current_session_info.session_id
         session_info['end time'] = str(self._current_session_info.cut_off_at)
         session_info['cutoff price'] = self._current_session_info.last_cut_off_price

      return session_info


   async def on_bitfinex_authenticated(self, auth_message):
      logging.info('================= Authenticated to bitfinex')
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

      total_balance = wallet.balance
      if wallet.balance_available is not None:
         free_balance = wallet.balance_available
         reserved_balance = wallet.balance - wallet.balance_available
      else:
         free_balance = None
         reserved_balance = None

      balances = {}

      balances['total'] = total_balance
      balances['free'] = free_balance
      balances['reserved'] = reserved_balance

      self._bitfinex_balances[wallet.type][wallet.currency] = balances

   async def _on_bitfinex_order_new(self, order):
      pass

   async def _on_bitfinex_order_confirmed(self, order):
      pass

   async def _on_bitfinex_order_closed(self, order):
      pass

   async def _set_collateral_on_position(self, product, target_collateral):
      if self._overseer_mode:
         return

      await self._bfx.rest.set_derivative_collateral(symbol=product, collateral=target_collateral)

   async def _update_position(self, position):
      self._bitfinex_positions[position.symbol] = position

      if self._overseer_mode:
         return

      # check collateral
      if position.symbol == self.bitfinex_futures_hedging_product and position.collateral is not None:
         amount = abs(position.amount)
         current_collateral = position.collateral

         if position.amount < 0:
            price = self.bitfinex_book.get_aggregated_bid_price(amount)
         else:
            price = self.bitfinex_book.get_aggregated_ask_price(amount)

         if price is None:
            return

         min_collateral = amount * price.price * self.min_bitfinex_leverage / 100
         max_collateral = amount * price.price * self.max_bitfinex_leverage / 100
         if current_collateral <= min_collateral or max_collateral <= current_collateral:
            target_collateral = amount * price.price * self.bitfinex_leverage / 100

            logging.info(f'Setting colateral to {target_collateral} from {current_collateral} for {self.bitfinex_futures_hedging_product}')
            await self._set_collateral_on_position(self.bitfinex_futures_hedging_product, target_collateral)

   async def _on_bitfinex_position_snapshot(self, raw_data):
      for data in raw_data[2]:
         position = Position.from_raw_rest_position(data)
         await self._update_position(position)
      await self._on_bitfinex_positions_loaded()

   async def _on_bitfinex_position_new(self, data):
      position = Position.from_raw_rest_position(data[2])
      await self._update_position(position)

   async def _on_bitfinex_position_update(self, data):
      position = Position.from_raw_rest_position(data[2])
      await self._update_position(position)

   async def _on_bitfinex_position_close(self, data):
      position = Position.from_raw_rest_position(data[2])
      logging.info(f'Position closed for {position.symbol}')
      self._bitfinex_positions[position.symbol] = None

   async def _on_bitfinex_margin_info_update(self, data):
      logging.info(f'======= on_bitfinex_margin_info_update: {data}')

   async def run(self):
      bitfinex_task = asyncio.create_task(self._bfx.ws.get_task_executable())
      leverex_task = asyncio.create_task(self._leverex_connection.run(self))
      status_server_task = asyncio.create_task(self._status_server.serve())

      done, pending = await asyncio.wait([bitfinex_task, leverex_task, status_server_task], return_when=asyncio.FIRST_COMPLETED)

   async def updateOffer(self):
      logging.info('===============  updateOffer =========')

   def _get_buying_power(self):
      if self._target_ccy_product in self.leverex_balances:
         return self.leverex_balances[self._target_ccy_product]

      return 0

   def _get_margin_reserved(self):
      if self._target_margin_product in self.leverex_balances:
         return self.leverex_balances[self._target_margin_product]

      return 0

   def _get_net_exposure(self):
      return self._net_exposure

   def _get_max_exposure(self):
      # no info on session. either closed or not loaded yet
      if self._current_session_info is None:
         return 0

      max_margin = (self._get_margin_reserved() + self._get_buying_power()) / 2
      margin_rate = self._current_session_info.last_cut_off_price / 10

      return max_margin / margin_rate

   def get_ask_offer_volume(self):
      max_exposure = self._get_max_exposure()
      if max_exposure == 0:
         return 0

      return max_exposure + self._get_net_exposure()

   def get_bid_offer_volume(self):
      max_exposure = self._get_max_exposure()

      if max_exposure == 0:
         return 0

      bid_volume = max_exposure - self._get_net_exposure()
      if bid_volume < 0:
         return 0

      return bid_volume

   async def submit_offers(self):
      if len(self.leverex_balances) == 0:
         return

      if self._overseer_mode:
         return

      ask_volume = self.get_ask_offer_volume()
      bid_volume = self.get_bid_offer_volume()

      if ask_volume == 0 and bid_volume == 0:
         return

      ask = self.bitfinex_book.get_aggregated_ask_price(ask_volume)
      bid = self.bitfinex_book.get_aggregated_bid_price(bid_volume)

      if ask is not None and bid is not None:
         # if bitfinex could not cover requested volume
         if ask.volume < ask_volume:
            ask_volume = ask.volume
         if bid.volume < bid_volume:
            bid_volume = bid.volume

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
         logging.info('Book is not loaded')

   def on_connected(self):
      pass

   async def on_authorized(self):
      await self._leverex_connection.load_open_positions(target_product=self.leverex_product, callback=self.on_positions_loaded)
      await self._leverex_connection.subscribe_session_open(self.leverex_product)
      await self._leverex_connection.load_deposit_address(callback=self.on_leverex_deposit_address_loaded)
      await self._leverex_connection.load_whitelisted_addresses(callback=self.on_leverex_addresses_loaded)

   def on_market_data(self, update):
      logging.info('on_market_data: {}'.format(update))

   def onLoadBalance(self, balances):
      logging.info('Balance loaded {}'.format(balances))
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
      for order in orders:
         self.store_active_order(order)
      await self._on_leverex_positions_loaded()
      logging.info(f'======== {len(orders)} posiions loaded')

   def on_leverex_deposit_address_loaded(self, address):
      if self._leverex_deposit_addresses is None:
         self._leverex_deposit_addresses = DepositWithdrawAddresses()

      self._leverex_deposit_addresses.set_deposit_address(address)
      self._validate_rebalance_feature_state()

   def on_leverex_addresses_loaded(self, addresses):
      if self._leverex_deposit_addresses is None:
         self._leverex_deposit_addresses = DepositWithdrawAddresses()

      self._leverex_deposit_addresses.set_withdraw_addresses(addresses, note)
      self._validate_rebalance_feature_state()

   async def _create_bitfinex_order(self, leverex_order):
      # negative amount is sell, positive is buy
      # we need to invert leverex side here
      if leverex_order.is_sell:
         quantity = leverex_order.quantity
      else:
         quantity = -leverex_order.quantity

      logging.info(f'Submitting order to bitfinex {quantity}')

      await self._send_bitfinex_order_request(quantity)

   async def _send_bitfinex_order_request(self, quantity):
      if self._overseer_mode:
         logging.error('Create Order on BF request ignored in overseer mode')
         return
      await self._bfx.ws.submit_order(symbol=self.bitfinex_futures_hedging_product,
                                      leverage=self.bitfinex_leverage,
                                      # this is a market order. price is ignored
                                      price=None,
                                      amount=quantity,
                                      market_type=BitfinexOrder.Type.MARKET)

   def store_active_order(self, order):
      self._positions[order.id] = order

      if not order.is_trade_position:
         # this position is rollover position
         # and it carries full exposure for a session
         logging.info('[store_active_order] Net exposure reseted before rollover')
         self._net_exposure = 0

      if order.is_sell:
         self._net_exposure = self._net_exposure - order.quantity
      else:
         self._net_exposure = self._net_exposure + order.quantity

      logging.info(f'[store_active_order] Net exposure : {self._net_exposure}')

   # position matched on leverex
   def on_order_created(self, order):
      if order.product_type == self.leverex_product:
         self.store_active_order(order)

         if not self._overseer_mode:
            if order.is_trade_position:
               # create order on bitfinex
               asyncio.create_task(self._create_bitfinex_order(order))
            else:
               logging.info(f'[on_order_created] get rollover position {order.quantity} {order._rollover_type}')
               asyncio.create_task(self._validate_position_size())

   async def _on_bitfinex_positions_loaded(self):
      self._bitfinex_position_loaded = True
      await self._validate_position_size()

   async def _on_leverex_positions_loaded(self):
      self._leverex_orders_loaded = True
      await self._validate_position_size()

   async def _validate_position_size(self):
      if self._overseer_mode:
         return

      if self._bitfinex_position_loaded and self._leverex_orders_loaded:
         bitfinex_position_size = 0
         if self._bitfinex_positions[self.bitfinex_futures_hedging_product] is not None:
            bitfinex_position_size = self._bitfinex_positions[self.bitfinex_futures_hedging_product].amount

         logging.info(f'[_validate_position_size] position : {bitfinex_position_size}, Net exposure {self._net_exposure}')

         inverted_leverex_exposure = -self._net_exposure
         position_diference = abs(inverted_leverex_exposure - bitfinex_position_size)
         # NOTE: acceptable difference 0.00001
         if position_diference > 0.00001:
            correction_quantity = inverted_leverex_exposure - bitfinex_position_size
            logging.info(f'Unexpected hedging position size: Leverex {inverted_leverex_exposure} != {bitfinex_position_size}. Correction order quantity {correction_quantity}')
            await self._send_bitfinex_order_request(quantity=correction_quantity)
            logging.info('============================')

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
                            'bitfinex_margin_wallet',
                            'price_ratio',
                            'min_bitfinex_leverage',
                            'bitfinex_leverage',
                            'max_bitfinex_leverage']
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
