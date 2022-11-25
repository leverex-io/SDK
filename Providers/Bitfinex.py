import logging
import asyncio

from Factories.Provider.Factory import Factory
from Factories.Definitions import ProviderException, \
   AggregationOrderBook

import .bitfinex-api-py.bfxapi as bfxapi

from bfxapi import Client
from bfxapi import Order
import bfxapi.models as bfx_models

class BitfinexProvider(Factory):
   def __init__(self, config):
      super().__init__()
      self.connection = None
      self.positions = {}
      self.balances = {}

      #check for required config entries
      required_settings = {
         'bitfinex': ['api_key', 'api_secret'],
         'hedging_settings': ['bitfinex_futures_hedging_product',
                              'bitfinex_orderbook_product',
                              'bitfinex_derivatives_currency',
                              'min_bitfinex_leverage',
                              'bitfinex_leverage',
                              'max_bitfinex_leverage',
                              'max_offer_volume']
      }
      for k in required_settings:
         if k not in config:
            logging.error(f'Missing {k} in config')
            exit(1)

         for kk in required_settings[k]:
            if kk not in config[k]:
               logging.error(f'Missing {kk} in config group {k}')
               exit(1)

      self.config = config['bitfinex']
      self.hedging_settings = config['hedging_settings']

      self.orderbook_product = self.hedging_settings['bitfinex_orderbook_product']
      self.derivatives_currency = self.hedging_settings['bitfinex_derivatives_currency']
      self.product = self.hedging_settings['bitfinex_futures_hedging_product']
      self.min_leverage = self.hedging_settings['min_bitfinex_leverage']
      self.leverage = self.hedging_settings['bitfinex_leverage']
      self.max_leverage = self.hedging_settings['max_bitfinex_leverage']
      self.max_offer_volume = self.hedging_settings['max_offer_volume']

      self.order_book_len = 100
      if 'bitfinex_order_book_len' in self.hedging_settings:
         self.order_book_len = self.hedging_settings['bitfinex_order_book_len']

      self.order_book_aggregation = 'P0'
      if 'bitfinex_order_book_aggregation' in self.hedging_settings:
         self.order_book_aggregation = self.hedging_settings['bitfinex_order_book_aggregation']

      # setup Bitfinex connection
      self.order_book = AggregationOrderBook()

   def setup(self, callback):
      super().setup(callback)

      log_level = 'INFO'
      if 'log_level' in self.config:
         log_level = self.config['log_level']
      self.connection = Client(API_KEY=self.config['api_key'],
         API_SECRET=self.config['api_secret'], logLevel=log_level)

      self.connection.ws.on('authenticated', self.on_authenticated)
      self.connection.ws.on('balance_update', self.on_balance_updated)
      self.connection.ws.on('wallet_snapshot', self.on_wallet_snapshot)
      self.connection.ws.on('wallet_update', self.on_wallet_update)
      self.connection.ws.on('order_book_update', self.on_order_book_update)
      self.connection.ws.on('order_book_snapshot', self.on_order_book_snapshot)

      self.connection.ws.on('order_new', self.on_order_new)
      self.connection.ws.on('order_confirmed', self.on_order_confirmed)
      self.connection.ws.on('order_closed', self.on_order_closed)
      self.connection.ws.on('position_snapshot', self.on_position_snapshot)
      self.connection.ws.on('position_new', self.on_position_new)
      self.connection.ws.on('position_update', self.on_position_update)
      self.connection.ws.on('position_close', self.on_position_close)
      self.connection.ws.on('margin_info_update', self.on_margin_info_update)

   def getAsyncIOTask(self):
      return asyncio.create_task(self.connection.ws.get_task_executable())

   #############################################################################
   #### events
   #############################################################################

   ## connection events ##
   async def on_authenticated(self, auth_message):
      logging.info('================= Authenticated to Bitfinex')
      self.ready = True

      try:
         deposit_address = await self.connection.rest.get_wallet_deposit_address(
            wallet=self.deposit_wallet, method=self._rebalance_method)
         self.deposit_addresses = DepositWithdrawAddresses()
         self.deposit_addresses.set_deposit_address(deposit_address.notify_info.address)
         self._validate_rebalance_feature_state()
      except Exception as e:
         logging.error(f'Failed to load Bitfinex deposit address: {str(e)}')

      # subscribe to order book
      await self.connection.ws.subscribe('book', self.orderbook_product,
         len=self.order_book_len, prec=self.order_book_aggregation)

   ## balance events ##
   async def on_balance_updated(self, data):
      self.balances['USD total'] = float(data[0])
      self.balances['USD Net'] = float(data[1])
      await super().onBalanceUpdate()

   def _explicitly_reset_derivatives_wallet(self):
      logging.info('Setting derivatives wallet balance to 0 explicitly')

      balances = {}

      balances['total'] = 0
      balances['free'] = 0
      balances['reserved'] = 0

      self.balances['margin'] = {}
      self.balances['margin'][self.derivatives_currency] = balances

   async def on_wallet_snapshot(self, wallets_snapshot):
      self._explicitly_reset_derivatives_wallet()

      for wallet in wallets_snapshot:
         await self.on_wallet_update(wallet)

   async def on_wallet_update(self, wallet):
      if wallet.type not in self.balances:
         self.balances[wallet.type] = {}

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

      self.balances[wallet.type][wallet.currency] = balances
      await super().onBalanceUpdate()

   ## order book events ##
   async def on_order_book_update(self, data):
      self.order_book.process_update(data['data'])
      await super().onOrderBookUpdate()

   def on_order_book_snapshot(self, data):
      self.order_book.setup_from_snapshot(data['data'])

   ## order events ##
   async def on_order_new(self, order):
      super().onNewOrder()

   async def on_order_confirmed(self, order):
      pass

   async def on_order_closed(self, order):
      pass

   ## position events ##
   async def on_position_snapshot(self, raw_data):
      for data in raw_data[2]:
         position = bfx_models.Position.from_raw_rest_position(data)
         await self.update_position(position)

   async def on_position_new(self, data):
      position = bfx_models.Position.from_raw_rest_position(data[2])
      await self.update_position(position)

   async def on_position_update(self, data):
      position = bfx_models.Position.from_raw_rest_position(data[2])
      await self.update_position(position)

   async def on_position_close(self, data):
      position = Position.from_raw_rest_position(data[2])
      logging.info(f'Position closed for {position.symbol}')
      self.positions[position.symbol] = None
      await super().onPositionUpdate()

   async def update_position(self, position):
      self.positions[position.symbol] = position
      await super().onPositionUpdate()

   ## margin events ##
   async def on_margin_info_update(self, data):
      logging.info(f'======= on_bitfinex_margin_info_update: {data}')

   #############################################################################
   #### methods
   #############################################################################

   ## volume ##
   def getOpenVolume(self):
      leverageRatio = self.leverage / 100
      priceBid = self.order_book.get_aggregated_bid_price(self.max_offer_volume)
      priceAsk = self.order_book.get_aggregated_ask_price(self.max_offer_volume)
      balance = self.balances['USD Net']

      #TODO: account for exposure that can be freed from current orders

      result = {}
      result["ask"] = balance / (leverageRatio * priceAsk.price)
      result["bid"] = balance / (leverageRatio * priceBid.price)
      return result

   ## exposure ##
   def getExposure(self):
      if self.product not in self.positions:
         return 0
      return self.positions[self.product].amount

   async def updateExposure(self, quantity):
      await self._bfx.ws.submit_order(symbol=self.product,
         leverage=self.leverage,
         price=None, # this is a market order, price is ignored
         amount=quantity,
         market_type=BitfinexOrder.Type.MARKET)