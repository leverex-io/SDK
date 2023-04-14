import asyncio
import json
import time
import websockets
import websockets.exceptions
import logging
import functools
from datetime import datetime

from typing import Callable

from .login_connection import LoginServiceClientWS
from Factories.Definitions import PriceOffer, \
   SessionCloseInfo, SessionOpenInfo, Order, WithdrawInfo, \
   DepositInfo

LOGIN_ENDPOINT = "wss://login-live.leverex.io/ws/v1/websocket"
API_ENDPOINT = "wss://api-live.leverex.io"

ORDER_ACTION_CREATED = 1
ORDER_ACTION_UPDATED = 2

ORDER_STATUS_PENDING = 1
ORDER_STATUS_FILLED  = 2

ORDER_TYPE_TRADE_POSITION                 = 0
ORDER_TYPE_NORMAL_ROLLOVER_POSITION       = 1
ORDER_TYPE_LIQUIDATED_ROLLOVER_POSITION   = 2
ORDER_TYPE_DEFAULTED_ROLLOVER_POSITION    = 3

################################################################################
class TradeHistory():
   def __init__(self, data):
      self._loaded = data['loaded']
      if self._loaded:
         self._orders = [LeverexOrder(order_data) for order_data in data['orders']]
         self._start_time = datetime.fromtimestamp(data['start_time'])
         self._end_time = datetime.fromtimestamp(data['end_time'])
      else:
         self._start_time = None
         self._end_time = None
         self._orders = None

   @property
   def loaded(self):
      return self._loaded

   @property
   def start_time(self):
      return self._start_time

   @property
   def end_time(self):
      return self._end_time

   @property
   def orders(self):
      return self._orders


################################################################################
class LeverexOrder(Order):
   def __init__(self, data):
      super().__init__(data['id'],
         data['timestamp'],
         float(data['quantity']),
         float(data['price']),
         int(data['side'])
      )

      self._status = int(data['status'])
      self._product_type = data['product_type']
      #self._cut_off_price = float(data['cut_off_price'])
      #self._trade_im = data['trade_im']
      self._trade_pnl = None
      self._reference_exposure = float(data['reference_exposure'])
      self._session_id = int(data['session_id'])
      self._rollover_type = data['rollover_type']
      self._fee = data['fee']
      self._is_taker = data['is_taker']

      self.indexPrice = None
      self.sessionIM = None

   def is_filled(self):
      return self._status == ORDER_STATUS_FILLED

   @property
   def product_type(self):
      return self._product_type

   @property
   def is_taker(self):
      return self._is_taker

   '''
   @property
   def cut_off_price(self):
      return self._cut_off_price

   @property
   def trade_im(self):
      return self._trade_im
   '''

   @property
   def trade_pnl(self):
      return self._trade_pnl

   @property
   def session_id(self):
      return self._session_id

   def is_trade_position(self):
      return self._rollover_type == ORDER_TYPE_TRADE_POSITION

   @property
   def is_rollover_liquidation(self):
      return self._rollover_type == ORDER_TYPE_LIQUIDATED_ROLLOVER_POSITION

   @property
   def is_rollover_default(self):
      return self._rollover_type == ORDER_TYPE_DEFAULTED_ROLLOVER_POSITION

   @property
   def fee(self):
      return self._fee

   @staticmethod
   def tradeTypeStr(tradeType):
      if tradeType == ORDER_TYPE_LIQUIDATED_ROLLOVER_POSITION:
         return "LIQUIDATED"
      elif tradeType == ORDER_TYPE_DEFAULTED_ROLLOVER_POSITION:
         return "DEFAULTED"
      return None

   def __str__(self):
      text = "<id: {} -- vol: {}, price: {} "
      tradeType = self.tradeTypeStr(self._rollover_type)
      if tradeType != None:
         text += "-- {}: {}".format(tradeType, \
            abs(self._reference_exposure) - self.quantity)
      text += ">"

      pl = self.trade_pnl
      if isinstance(pl, float):
         pl = round(pl, 6)

      vol = self.quantity
      if self.is_sell():
         vol *= -1

      return text.format(self.id, vol, self.price, tradeType)

   def setSessionIM(self, session):
      if session == None:
         return
      if self.session_id != session.getSessionId():
         return

      self.sessionIM = session.getSessionIM()

   def setIndexPrice(self, price):
      self.indexPrice = price
      self.computePnL()

   def computePnL(self):
      if self.indexPrice == None or self.sessionIM == None:
         self._trade_pnl = None
         return

      #calculate difference between entry and index price
      #cap by max nominal move
      priceDelta = abs(self.indexPrice - self.price)
      priceDelta = min(priceDelta, self.sessionIM)

      #apply operation sign
      if self.price > self.indexPrice:
         priceDelta = -priceDelta

      #apply side sign
      if self.is_sell():
         priceDelta = -priceDelta

      #set pnl
      self._trade_pnl = self.quantity * priceDelta

   def getMargin(self):
      if self.sessionIM == None:
         return None

      return self.sessionIM * self.quantity

PriceOffers = list[PriceOffer]

################################################################################
class AsyncApiConnection(object):
   def __init__(self, api_endpoint=API_ENDPOINT,
      login_endpoint=LOGIN_ENDPOINT,
      key_file_path=None,
      dump_communication=False,
      email=None):

      self._dump_communication = dump_communication

      self._login_client = None
      self.access_token = None

      self._api_endpoint = api_endpoint
      self._login_endpoint = login_endpoint

      if key_file_path is not None:
         self._login_client = LoginServiceClientWS(
            private_key_path=key_file_path,
            login_endpoint=login_endpoint,
            email=email,
            dump_communication=dump_communication)

      self.websocket = None
      self.listener = None

      # setup write queue
      self.write_queue = asyncio.Queue()

      self._requests_cb = {}

   async def _call_listener_cb(self, cb, *args, **kwargs):
      if asyncio.iscoroutinefunction(cb):
         await cb(*args, **kwargs)
      else:
         cb(*args, **kwargs)

   async def _call_listener_method(self, method_name: str, *args, **kwargs):
      listener_cb = getattr(self.listener, method_name, None)
      if callable(listener_cb):
         await self._call_listener_cb(listener_cb, *args, **kwargs)
      else:
         logging.error(f'{method_name} not defined in listener')

   def _generate_reference_id(self):
      return str(round(time.time() * 1000000))

   def loadBalances(self, callback):
      reference = self._generate_reference_id()
      loadBalanceRequest = {
         'load_balance' : {
         'reference': reference
      }}
      if callback is not None:
         self._requests_cb[reference] = callback
      self.write_queue.put_nowait(json.dumps(loadBalanceRequest))

   async def load_deposit_address(self, callback: Callable = None):
      reference = self._generate_reference_id()

      load_deposit_address_request = {
         'load_deposit_address' : {
            'reference' : reference
         }
      }

      if callback is not None:
         self._requests_cb[reference] = callback
      else:
         listener_cb = getattr(self.listener, 'on_deposit_address_loaded', None)
         if callable(listener_cb):
            self._requests_cb[reference] = listener_cb

      await self.websocket.send(json.dumps(load_deposit_address_request))

   async def load_trade_history(self, target_product, limit=0, offset=0, start_time: datetime = None
                                , end_time: datetime = None, callback: Callable = None):
      reference = self._generate_reference_id()

      load_trades_request = {
         'trade_history': {
            'limit': limit,
            'offset': offset,
            'start_time': (int(start_time.timestamp()) if start_time is not None else 0),
            'end_time': (int(end_time.timestamp()) if end_time is not None else 0),
            'product_type': target_product,
            'reference': reference
         }
      }

      if callback is not None:
         self._requests_cb[reference] = callback
      else:
         listener_cb = getattr(self.listener, 'on_trade_history_loaded', None)
         if callable(listener_cb):
            self._requests_cb[reference] = listener_cb

      await self.websocket.send(json.dumps(load_trades_request))

   async def load_withdrawals_history(self, callback: Callable = None):
      reference = self._generate_reference_id()

      load_withdrawals_history_request = {
         'load_withdrawals': {
            'reference': reference
         }
      }
      if callback is not None:
         self._requests_cb[reference] = callback
      else:
         listener_cb = getattr(self.listener, 'on_withdrawals_history_loaded', None)
         if callable(listener_cb):
            self._requests_cb[reference] = listener_cb

      await self.websocket.send(json.dumps(load_withdrawals_history_request))

   async def load_deposits_history(self, callback: Callable = None):
      reference = self._generate_reference_id()

      load_deposits_history_request = {
         'load_deposits': {
            'reference': reference
         }
      }

      if callback is not None:
         self._requests_cb[reference] = callback
      else:
         listener_cb = getattr(self.listener, 'on_deposits_history_loaded', None)
         if callable(listener_cb):
            self._requests_cb[reference] = listener_cb

      await self.websocket.send(json.dumps(load_deposits_history_request))

   async def withdraw_liquid(self, *, address, currency, amount, callback: Callable = None):
      reference = self._generate_reference_id()

      withdraw_request = {
            'withdraw_liquid': {
            'address': str(address),
            'currency': str(currency),
            'amount': str(amount),
            'reference': reference
         }
      }

      if callback is not None:
         self._requests_cb[reference] = callback
      else:
         listener_cb = getattr(self.listener, 'on_withdraw_request_response', None)
         if callable(listener_cb):
            self._requests_cb[reference] = listener_cb
         else:
            logging.error(f'No callback set for withdraw_liquid request {reference}')

      await self.websocket.send(json.dumps(withdraw_request))

   async def cancel_withdraw(self, *, id, callback: Callable = None):
      reference = self._generate_reference_id()

      cancel_withdraw = {
         'cancel_withdraw': {
            'id': id,
            'reference': reference
         }
      }
      if callback is not None:
         self._requests_cb[reference] = callback
      await self.websocket.send(json.dumps(cancel_withdraw))

   async def load_whitelisted_addresses(self, callback: Callable = None):
      reference = self._generate_reference_id()

      load_whitelisted_addresses_request = {
         'load_addresses': {
            'reference': reference
         }
      }

      if callback is not None:
         self._requests_cb[reference] = callback
      else:
         listener_cb = getattr(self.listener, 'on_whitelisted_addresses_loaded', None)
         if callable(listener_cb):
            self._requests_cb[reference] = listener_cb

      await self.websocket.send(json.dumps(load_whitelisted_addresses_request))

   # callback(orders: list[Order] )
   async def load_open_positions(self, target_product, callback: Callable = None):
      reference = self._generate_reference_id()

      load_positions_request = {
         'load_orders': {
            'product_type': target_product,
            'reference': reference
         }
      }

      if callback is not None:
         self._requests_cb[reference] = callback
      else:
         self._requests_cb[reference] = functools.partial(self.listener.on_load_positions, target_product=target_product)

      await self.websocket.send(json.dumps(load_positions_request))

   async def submit_offers(self, target_product: str, offers: PriceOffers, callback: Callable = None):
      price_offers = [offer.to_map() for offer in offers if offer.to_map() is not None]

      reference = self._generate_reference_id()

      submit_prices_request = {
         'submit_prices': {
            'product_type': target_product,
            'prices': price_offers,
            'reference': reference
         }
      }

      if callback is not None:
         self._requests_cb[reference] = callback
      else:
         listener_cb = getattr(self.listener, 'onSubmitPrices', None)
         if callable(listener_cb):
            self._requests_cb[reference] = listener_cb

      await self.websocket.send(json.dumps(submit_prices_request))

   async def subscribe_session_open(self, target_product: str):
      subscribe_request = {
         'session_open' : {
            'product_type' : target_product
         }
      }
      await self.websocket.send(json.dumps(subscribe_request))

   async def subscribe_to_product(self, target_product: str):
      subscribe_request = {
         'subscribe': {
            'product_type': target_product
         }
      }
      await self.websocket.send(json.dumps(subscribe_request))

   async def login(self):
      #get token from login server
      access_token_info = await self._login_client.get_access_token(self._api_endpoint)
      if access_token_info == None:
         raise Exception("Failed to get access token")

      #submit to service
      self.access_token = access_token_info
      auth_request = {
         'authorize': {
            'token': access_token_info['access_token']
         }
      }

      await self.websocket.send(json.dumps(auth_request))
      data = await self.websocket.recv()
      loginResult = json.loads(data)
      if not 'authorize' in loginResult or not loginResult['authorize']['success']:
         raise Exception("Login failed")
      print (f"-- LOGGED IN AS: {loginResult['authorize']['email']}")

   async def run(self, listener):
      self.listener = listener

      def send_data(data):
         self.write_queue.put_nowait(json.dumps(data))
      self.listener.send = send_data

      async with websockets.connect(self._api_endpoint) as self.websocket:
         await self._call_listener_method('on_connected')

         if self._login_client is not None:
            await self.login()
            await self._call_listener_method('on_authorized')

         # start the loops
         readTask = asyncio.create_task(self.readLoop(), name="Leverex Read task")
         writeTask = asyncio.create_task(self.writeLoop(), name="Leverex write task")

         if self._login_client is not None:
            cycleTask = asyncio.create_task(self.cycleSession(), name="Leverex login cycle task")

         await readTask
         await writeTask

         if self._login_client is not None:
            await cycleTask

   async def readLoop(self):
      while True:
         data = await self.websocket.recv()
         if data is None:
            continue
         update = json.loads(data)

         if 'market_data' in update:
            await self.listener.on_market_data(update['market_data'])

         elif 'load_balance' in update:
            if 'reference' in update['load_balance'] and \
               update['load_balance']['reference'] in self._requests_cb:
               cb = self._requests_cb.pop(update['load_balance']['reference'])
               await self._call_listener_cb(cb, update['load_balance']['balances'])
            else:
               await self._call_listener_method('onLoadBalance', update['load_balance']['balances'])

         elif 'subscribe' in update:
            if not update['subscribe']['success']:
               raise Exception('Failed to subscribe to prices: {}'.format(update['subscribe']['error_msg']))

         elif 'submit_prices' in update:
            reference = update['submit_prices']['reference']
            if reference in self._requests_cb:
               cb = self._requests_cb.pop(reference)
               await self._call_listener_cb(cb, update)
            else:
               logging.error(f'submit_prices response with unregistered request reference:{reference}')

         elif 'withdraw_liquid' in update:
            reference = update['withdraw_liquid']['reference']

            if reference in self._requests_cb:
               withdraw_info = WithdrawInfo(update['withdraw_liquid'])
               cb = self._requests_cb.pop(reference)
               await self._call_listener_cb(cb, withdraw_info)
            else:
               logging.error(f'withdraw_liquid response with unregistered request reference:{reference}')

         elif 'cancel_withdraw' in update:
            reference = update['cancel_withdraw']['reference']
            withdraw_info = WithdrawInfo(update['cancel_withdraw'])
            if reference in self._requests_cb:
               cb = self._requests_cb.pop(reference)
               await self._call_listener_cb(cb, withdraw_info)
            else:
               logging.error(f'cancel_withdraw response with unregistered request reference:{reference}')

         elif 'load_addresses' in update:
            reference = update['load_addresses']['reference']

            addresses = {}

            for entry in update['load_addresses']['addresses']:
               addresses[entry['address']] = entry['description']

            if reference in self._requests_cb:
               cb = self._requests_cb.pop(reference)
               await self._call_listener_cb(cb, addresses)
            else:
               logging.error(f'load_addresses response with unregistered request reference:{reference}')

         elif 'load_deposit_address' in update:
            reference = update['load_deposit_address']['reference']

            address = update['load_deposit_address']['address']

            if reference in self._requests_cb:
               cb = self._requests_cb.pop(reference)
               await self._call_listener_cb(cb, address)
            else:
               logging.error(f'load_deposit_address response with unregistered request reference:{reference}')

         elif 'trade_history' in update:
            reference = update['trade_history']['reference']

            if reference in self._requests_cb:
               cb = self._requests_cb.pop(reference)

               trade_history = TradeHistory(update['trade_history'])

               await self._call_listener_cb(cb, trade_history)
            else:
               logging.error(f'trade_history response with unregistered request reference:{reference}')

         elif 'load_withdrawals' in update:
            reference = update['load_withdrawals']['reference']
            if reference in self._requests_cb:
               cb = self._requests_cb.pop(reference)
               withdrawals = [WithdrawInfo(entry) for entry in update['load_withdrawals']['withdrawals']]
               await self._call_listener_cb(cb, withdrawals)
            else:
               logging.error(f'load_withdrawals response with unregistered request reference:{reference}')

         elif 'load_deposits' in update:
            reference = update['load_deposits']['reference']
            if reference in self._requests_cb:
               cb = self._requests_cb.pop(reference)

               deposits = [DepositInfo(entry) for entry in update['load_deposits']['deposits']]

               await self._call_listener_cb(cb, deposits)
            else:
               logging.error(f'load_deposits response with unregistered request reference:{reference}')

         elif 'load_orders' in update:
            orders = [LeverexOrder(order_data) for order_data in update['load_orders']['orders']]
            reference = update['load_orders']['reference']

            if reference in self._requests_cb:
               cb = self._requests_cb.pop(reference)
               await self._call_listener_cb(cb, orders)
            else:
               logging.error(f'load_orders response with unregistered request  reference:{reference}')

         elif 'order_update' in update:
            order = LeverexOrder(update['order_update']['order'])
            action = int(update['order_update']['action'])
            await self.listener.on_order_event(order, action)

         # _call_listener_method
         elif 'update_deposit' in update:
            deposit_info = DepositInfo(update['update_deposit'])
            await self._call_listener_method('on_deposit_update', deposit_info)

         elif 'update_withdrawal' in update:
            withdraw_info = WithdrawInfo(update['update_withdrawal'])
            await self._call_listener_method('on_withdraw_update', withdraw_info)

         elif 'session_closed' in update:
            await self._call_listener_cb(self.listener.on_session_closed, SessionCloseInfo(update['session_closed']))

         elif 'session_open' in update:
            await self._call_listener_cb(self.listener.on_session_open, SessionOpenInfo(update['session_open']))

         elif 'authorize' in update:
            if not update['authorize']['success']:
               raise Exception('Failed to renew session token')
         elif 'logout' in update:
            raise Exception('ERROR: we got a logout message. Closing connection')
         else:
            logging.warning('!!! Ignore update\n{} !!!'.format(update))

   async def writeLoop(self):
      while True:
         write_data = await self.write_queue.get()
         await self.websocket.send(write_data)

   async def cycleSession(self):
      while True:
         # wait for token lifetime - 1min
         await asyncio.sleep(self.access_token['expires_in'] * 0.9)

         # cycle token with login server
         self.access_token = await self._login_client.update_access_token(  # noqa: E111
            self.access_token['access_token'])

         # send to service
         auth_request = {
            'authorize': {
               'token': self.access_token['access_token']
            }
         }

         await self.websocket.send(json.dumps(auth_request))
