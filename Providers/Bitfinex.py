import logging
import asyncio

from Factories.Provider.Factory import Factory
from Factories.Definitions import ProviderException, \
   AggregationOrderBook

from Providers.bfxapi.bfxapi import Client
from Providers.bfxapi.bfxapi import Order
import Providers.bfxapi.bfxapi.models as bfx_models

BFX_USD_NET = 'USD Net'
BFX_USD_TOTAL = 'USD total'
BFX_DERIVATIVES_WALLET = 'margin'
BFX_DEPOSIT_METHOD = 'TETHERUSL'

################################################################################
class BitfinexException(Exception):
   pass

################################################################################
class DepositWithdrawAddresses():
   def __init__(self):
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

################################################################################
class BitfinexProvider(Factory):
   required_settings = {
      'bitfinex': [
         'api_key', 'api_secret',
         'orderbook_product',
         'derivatives_currency',
         'futures_hedging_product',
         'min_leverage',
         'leverage',
         'max_leverage',
      ],
      'hedging_settings': [
         'max_offer_volume'
      ]
   }

   #############################################################################
   #### setup
   #############################################################################
   def __init__(self, config):
      super().__init__("Bitfinex")
      self.connection = None
      self.positions = {}
      self.balances = {}
      self.lastReadyState = False

      #check for required config entries
      #check for required config entries
      for k in self.required_settings:
         if k not in config:
            raise BitfinexException(f'Missing \"{k}\" in config')

         for kk in self.required_settings[k]:
            if kk not in config[k]:
               raise BitfinexException(f'Missing \"{kk}\" in config group \"{k}\"')

      self.config = config['bitfinex']
      self.orderbook_product = self.config['orderbook_product']
      self.derivatives_currency = self.config['derivatives_currency']
      self.product = self.config['futures_hedging_product']
      self.min_leverage = self.config['min_leverage']
      self.leverage = self.config['leverage']
      self.max_leverage = self.config['max_leverage']
      self.max_offer_volume = config['hedging_settings']['max_offer_volume']

      self.order_book_len = 100
      if 'order_book_len' in self.config:
         self.order_book_len = self.config['order_book_len']

      self.order_book_aggregation = 'P0'
      if 'order_book_aggregation' in self.config:
         self.order_book_aggregation = self.config['order_book_aggregation']

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

   #############################################################################
   #### events
   #############################################################################

   ## connection events ##
   async def on_authenticated(self, auth_message):
      logging.info('================= Authenticated to Bitfinex')
      await super().setConnected(True)

      try:
         deposit_address = await self.connection.rest.get_wallet_deposit_address(
            wallet=BFX_DERIVATIVES_WALLET, method=BFX_DEPOSIT_METHOD)
         self.deposit_addresses = DepositWithdrawAddresses()
         self.deposit_addresses.set_deposit_address(deposit_address.notify_info.address)

         #check dealer rebalance feature readyness, should live in dealer, not bfx provider
         #self._validate_rebalance_feature_state()
      except Exception as e:
         logging.error(f'Failed to load Bitfinex deposit address: {str(e)}')

      # subscribe to order book
      await self.connection.ws.subscribe('book', self.orderbook_product,
         len=self.order_book_len, prec=self.order_book_aggregation)

   ## balance events ##
   async def on_balance_updated(self, data):
      '''
      This balance update has little use and only serves as a
      vague indicative value. We trigger specific notifications
      on updates to the bfx reserved wallet names instead.
      '''
      self.balances[BFX_USD_TOTAL] = float(data[0])
      self.balances[BFX_USD_NET] = float(data[1])

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
      await super().setInitBalance()
      await self.evaluateReadyState()

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
      if wallet.type == BFX_DERIVATIVES_WALLET:
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
      await super().setInitPosition()
      await self.evaluateReadyState()

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
   #### Provider overrides
   #############################################################################

   ## setup ##
   def getAsyncIOTask(self):
      return asyncio.create_task(self.connection.ws.get_task_executable())

   ## state ##
   def isReady(self):
      return self.lastReadyState

   ## volume ##
   def getOpenVolume(self):
      if not self.isReady():
         return None

      if BFX_DERIVATIVES_WALLET not in self.balances or \
         self.derivatives_currency not in self.balances[BFX_DERIVATIVES_WALLET]:
         logging.warning("Missing margin wallet balance")
         return None
      balance = self.balances[BFX_DERIVATIVES_WALLET][self.derivatives_currency]
      #TODO: account for exposure that can be freed from current orders

      leverageRatio = self.leverage / 100
      priceBid = self.order_book.get_aggregated_bid_price(self.max_offer_volume)
      priceAsk = self.order_book.get_aggregated_ask_price(self.max_offer_volume)

      if priceBid == None or priceAsk == None:
         return None

      result = {}
      result["ask"] = balance['free'] / (leverageRatio * priceAsk.price)
      result["bid"] = balance['free'] / (leverageRatio * priceBid.price)
      return result

   ## exposure ##
   def getExposure(self):
      if not self.isReady():
         return None

      if self.product not in self.positions:
         return 0
      return self.positions[self.product].amount

   async def updateExposure(self, quantity):
      await self.connection.ws.submit_order(symbol=self.product,
         leverage=self.leverage,
         price=None, # this is a market order, price is ignored
         amount=quantity,
         market_type=bfx_models.order.OrderType.MARKET)

   #############################################################################
   #### state
   #############################################################################
   async def evaluateReadyState(self):
      currentReadyState = super().isReady()
      if self.lastReadyState == currentReadyState:
         return

      self.lastReadyState = currentReadyState
      await super().onReady()
