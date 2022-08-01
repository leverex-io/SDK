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

LOGIN_ENDPOINT="wss://login-live.leverex.io/ws/v1/websocket"
API_ENDPOINT="wss://api-live.leverex.io"

ORDER_ACTION_CREATED = 1
ORDER_ACTION_UPDATED = 2

ORDER_STATUS_PENDING = 1
ORDER_STATUS_FILLED  = 2

SIDE_BUY    = 1
SIDE_SELL   = 2

ORDER_TYPE_TRADE_POSITION                 = 0
ORDER_TYPE_NORMAL_ROLLOVER_POSITION       = 1
ORDER_TYPE_LIQUIDATED_ROLLOVER_POSITION   = 2
ORDER_TYPE_DEFAULTED_ROLLOVER_POSITION    = 3

class SessionOpenInfo():
   def __init__(self, data):
      self.product_type = data['product_type']
      self.cut_off_at = datetime.fromtimestamp(data['cut_off_at'])
      self.last_cut_off_price = float(data['last_cut_off_price'])
      self.session_id = data['session_id']
      self.previous_session_id = data['previous_session_id']

class SessionCloseInfo():
   def __init__(self, data):
      self.product_type = data['product_type']
      self.session_id = data['session_id']

class Order():
   def __init__(self, data):
      self._id = data['id']
      self._status = int(data['status'])
      self._timestamp = data['timestamp']
      self._product_type = data['product_type']
      self._side = int(data['side'])
      self._quantity = float(data['quantity'])
      self._price = float(data['price'])
      self._cut_off_price = float(data['cut_off_price'])
      self._trade_im = data['trade_im']
      self._trade_pnl = data['trade_pnl']
      self._reference_exposure = data['reference_exposure']
      self._session_id = int(data['session_id'])
      self._rollover_type = data['rollover_type']
      self._fee = data['fee']

   @property
   def id(self):
      return self._id

   @property
   def is_filled(self):
      return self._status == ORDER_STATUS_FILLED

   @property
   def timestamp(self):
      return self._timestamp

   @property
   def product_type(self):
      return self._product_type

   @property
   def is_sell(self):
      return self._side == SIDE_SELL

   @property
   def quantity(self):
      return self._quantity

   @property
   def price(self):
      return self._price

   @property
   def cut_off_price(self):
      return self._cut_off_price

   @property
   def trade_im(self):
      return self._trade_im

   @property
   def trade_pnl(self):
      return self._trade_pnl

   @property
   def total_net_exposure(self):
      if self.is_trade_position:
         return self._reference_exposure
      return self.quantity

   @property
   def session_id(self):
      return self._session_id

   @property
   def is_trade_position(self):
      return self._rollover_type == ORDER_TYPE_TRADE_POSITION

   @property
   def fee(self):
      return self._fee

class WithdrawInfo():
   WITHDRAW_FAILED      = 0
   WITHDRAW_ACCEPTED    = 1
   WITHDRAW_PENDING     = 2
   WITHDRAW_BROADCASTED = 3
   WITHDRAW_COMPLETED   = 4
   WITHDRAW_CANCELLED   = 5
   WITHDRAW_BATCHED     = 6

   status_text = {
      WITHDRAW_FAILED : 'failed',
      WITHDRAW_ACCEPTED : 'accepted',
      WITHDRAW_PENDING : 'pending',
      WITHDRAW_BROADCASTED : 'broadcasted',
      WITHDRAW_COMPLETED : 'completed',
      WITHDRAW_CANCELLED : 'cancelled',
      WITHDRAW_BATCHED : 'batched'
   }

   def __init__(self, data):
      self._id = str(data['id'])
      self._tx_id = str(data['tx_id'])
      self._status = int(data['status'])
      self._recv_address = str(data['recv_address'])
      self._currency = str(data['currency'])
      self._amount = str(data['amount'])
      self._timestamp = datetime.fromtimestamp(data['timestamp'])
      self._unblinded_link = str(data['unblinded_link'])

   @property
   def id(self):
      return self._id

   @property
   def status_code(self):
      return self._status

   @property
   def status(self):
      return self.status_text.get(self._status, "Undefined")

   @property
   def recv_address(self):
      return self._recv_address

   @property
   def currency(self):
      return self._currency

   @property
   def amount(self):
      return self._amount

   @property
   def timestamp(self):
      return self._timestamp

   @property
   def unblinded_link(self):
      return self._unblinded_link

   @property
   def transacion_id(self):
      return self._tx_id

class DepositInfo():
   def __init__(self, data):
      self._tx_id = str(data['tx_id'])
      self._nb_conf = int(data['nb_conf'])
      self._unblinded_link = str(data['unblinded_link'])
      self._timestamp = datetime.fromtimestamp(data['timestamp'])

   @property
   def transacion_id(self):
      return self._tx_id

   @property
   def confirmations_count(self):
      return self._nb_conf

   @property
   def unblinded_link(self):
      return self._unblinded_link

   @property
   def timestamp(self):
      return self._timestamp

class PriceOffer():
   def __init__(self, volume, ask=None, bid=None):
      self._volume = volume
      self._ask = ask
      self._bid = bid

   @property
   def volume(self):
      return self._volume

   @property
   def ask(self):
      return self._ask

   @property
   def bid(self):
      return self._bid

   def to_map(self):
      if self._ask is None and self._bid is None:
         return None

      result = {}

      result['volume'] = str(self._volume)
      if self._ask is not None:
         result['ask'] = str(self._ask)
      if self._bid is not None:
         result['bid'] = str(self._bid)

      return result

PriceOffers = list[PriceOffer]

class AsyncApiConnection(object):
   def __init__(self, api_endpoint=API_ENDPOINT, login_endpoint=LOGIN_ENDPOINT,
                customer_email=None, key_file_path=None, dump_communication=False):

      self._dump_communication = dump_communication

      self._login_client = None
      self.access_token = None

      self._api_endpoint = api_endpoint
      self._login_endpoint = login_endpoint

      if key_file_path is not None and customer_email is not None:
         self._login_client = LoginServiceClientWS(email=customer_email,
                                                  private_key_path=key_file_path,
                                                  login_endpoint=login_endpoint,
                                                  dump_communication=dump_communication)

      self.websocket = None
      self.listener = None

      #setup write queue
      self.write_queue = asyncio.Queue()

      self._requests_cb = {}

   async def _call_listener_cb(self, cb, *args, **kwargs):
      if asyncio.iscoroutinefunction(cb):
         await cb(*args, **kwargs)
      else:
         cb(*args, **kwargs)

   async def _call_listener_method(self, method_name: str,  *args, **kwargs):
      listener_cb = getattr(self.listener, method_name, None)
      if callable(listener_cb):
         await self._call_listener_cb(listener_cb, *args, **kwargs)
      else:
         logging.error(f'{method_name} not defined in listener')

   def _generate_reference_id(self):
      return str(round(time.time() * 1000000))

   def loadBalances(self):
      loadBalanceRequest = {
         'load_balance' : {}
      }
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

   async def load_withdrawals_history(self, callback: Callable = None):
      reference = self._generate_reference_id()

      load_withdrawals_history_request = {
         'load_withdrawals' : {
            'reference' : reference
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
         'load_deposits' : {
            'reference' : reference
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
            'withdraw_liquid' : {
            'address' : str(address),
            'currency' : str(currency),
            'amount' : str(amount),
            'reference' : reference
         }
      }

      if callback is not None:
         self._requests_cb[reference] = callback
      else:
         listener_cb = getattr(self.listener, 'on_withdraw_request_response', None)
         if callable(listener_cb):
            self._requests_cb[reference] = listener_cb

      await self.websocket.send(json.dumps(withdraw_request))

   async def load_whitelisted_addresses(self, callback: Callable = None):
      reference = self._generate_reference_id()

      load_whitelisted_addresses_request = {
         'load_addresses' : {
            'reference' : reference
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
         'load_orders' : {
            'product_type' : target_product,
            'reference' : reference
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
         'submit_prices' : {
            'product_type' : target_product,
            'prices' : price_offers,
            'reference' : reference
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
         'subscribe' : {
            'product_type' : target_product
         }
      }
      await self.websocket.send(json.dumps(subscribe_request))

   async def login(self):
      #get token from login server
      access_token_info = await self._login_client.get_access_token(self._api_endpoint)

      #submit to service
      self.access_token = access_token_info
      auth_request = {
         'authorize' : {
            'token' : access_token_info['access_token']
         }
      }

      await self.websocket.send(json.dumps(auth_request))
      data = await self.websocket.recv()
      loginResult = json.loads(data)
      if not loginResult['authorize']['success']:
         raise Exception("Login failed")

   async def run(self, listener):
      self.listener = listener

      def send_data(data):
         self.write_queue.put_nowait(json.dumps(data))
      self.listener.send = send_data

      async with websockets.connect(self._api_endpoint) as self.websocket:
         self.listener.on_connected()

         if self._login_client is not None:
            await self.login()

            await self._call_listener_method('on_authorized')

            if asyncio.iscoroutinefunction(self.listener.on_authorized):
               await self.listener.on_authorized()
            else:
               self.listener.on_authorized()

            # load balances
            self.loadBalances()

         #start the loops
         readTask = asyncio.create_task(self.readLoop(), name="Leverex Read task")
         writeTask = asyncio.create_task(self.writeLoop(), name="Leverex write task")

         if self._login_client is not None:
            cycleTask = asyncio.create_task(self.cycleSession(), name="Leverex login cycle task")
            updateTask = asyncio.create_task(self.listener.updateOffer(), name="Leverex update offers taks")

         await readTask
         await writeTask

         if self._login_client is not None:
            await cycleTask
            await updateTask

   async def readLoop(self):
      balance_awaitable = False
      while True:
         data = await self.websocket.recv()
         if data is None:
            continue
         update = json.loads(data)

         if 'market_data' in update:
            self.listener.on_market_data(update['market_data'])

         elif 'load_balance' in update:
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
               await self._call_listener_cb(cb, withdraw_info)
            else:
               logging.error(f'withdraw_liquid response with unregistered request reference:{reference}')

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
            orders = [Order(order_data) for order_data in update['load_orders']['orders']]
            reference = update['load_orders']['reference']

            if reference in self._requests_cb:
               cb = self._requests_cb.pop(reference)
               await self._call_listener_cb(cb, orders)
            else:
               logging.error(f'load_orders response with unregistered request  reference:{reference}')

         elif 'order_update' in update:
            order = Order(update['order_update']['order'])
            action = int(update['order_update']['action'])
            if action == ORDER_ACTION_CREATED:
               self.listener.on_order_created(order)
            elif action == ORDER_ACTION_UPDATED:
               self.listener.on_order_filled(order)

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
            logging.warning('Ignore update\n{}'.format(update))

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
            'authorize' : {
               'token' : self.access_token['access_token']
            }
         }

         await self.websocket.send(json.dumps(auth_request))
