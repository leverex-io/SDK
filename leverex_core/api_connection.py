import asyncio
import json
import time
import websockets
import websockets.exceptions
import logging
import functools
import random
from datetime import datetime

from typing import Callable

from .login_connection import LoginServiceClientWS
from .utils import PriceOffer, \
   SessionCloseInfo, SessionOpenInfo, \
   Order, WithdrawInfo, DepositInfo, \
   SIDE_BUY, SIDE_SELL, DealerOffers, LeverexOrder, \
   DepositInfo, WithdrawInfo, TradeHistory

####
PriceOffers = list[PriceOffer]

################################################################################
class AsyncApiConnection(object):
   def __init__(self, api_endpoint, login_endpoint,
      key_file_path=None,
      dump_communication=False,
      email=None,
      aeid_endpoint=None):

      self._dump_communication = dump_communication

      self._login_client = None
      self.access_token = None

      self._api_endpoint = api_endpoint
      self._login_endpoint = login_endpoint

      self._login_client = LoginServiceClientWS(
         private_key_path=key_file_path,
         login_endpoint=login_endpoint,
         email=email,
         dump_communication=dump_communication,
         aeid_endpoint=aeid_endpoint)

      self.websocket = None
      self.listener = None
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
      return str(random.randint(0, 2**32-1))

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

   async def load_trade_history(self, target_product,
      limit=0, offset=0, start_time: datetime = None,
      end_time: datetime = None, callback: Callable = None):

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

   async def submit_prices(self, target_product: str, offers: PriceOffers, callback: Callable = None):
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

   async def subscribe_to_balance_updates(self, target_product: str):
      subscribe_request = {
         'load_balance' : {
            'product_type': target_product,
      }}
      await self.websocket.send(json.dumps(subscribe_request))

   async def subscribe_dealer_offers(self, product: str):
      subscribe_request = {
         'subscribe_dealer_offers' : {
            'product_type': product,
      }}
      await self.websocket.send(json.dumps(subscribe_request))

   async def place_order(self, amount: float, side, product: str, price: float):
      async def handleReply(reply):
         if reply['success'] == False:
            print (f"order failed with error: {reply['error_msg']}")

      reference = self._generate_reference_id()
      market_order = {
         'market_order' : {
            'amount': str(amount),
            'side': side,
            'product_type': product,
            'reference' : reference
         }
      }

      self._requests_cb[reference] = handleReply
      await self.websocket.send(json.dumps(market_order))

   async def product_fee(self, product: str, cb):
      reference = self._generate_reference_id()
      product_fee = {
         'product_fee' : {
            'product_type': product,
            'reference' : reference
         }
      }
      self._requests_cb[reference] = cb
      await self.websocket.send(json.dumps(product_fee))

   async def login(self):
      #get token from login server
      print ("logging in...")
      access_token_info = await self._login_client.logMeIn(self._api_endpoint)
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

      try:
         async with websockets.connect(self._api_endpoint) as self.websocket:
            await self._call_listener_method('on_connected')

            if self._login_client is not None:
               await self.login()
               await self._call_listener_method('on_authorized')

            # start the loops
            readTask = asyncio.create_task(self.readLoop(), name="Leverex Read task")
            if self._login_client is not None:
               cycleTask = asyncio.create_task(self.cycleSession(), name="Leverex login cycle task")
            await readTask

            if self._login_client is not None:
               await cycleTask
      except Exception as e:
         print(f"leverex_core/api_connection failed with error: {e}")
         loop = asyncio.get_running_loop()
         loop.stop()
         return

   async def readLoop(self):
      while True:
         data = await self.websocket.recv()
         if data is None:
            continue
         update = json.loads(data)

         if 'market_data' in update:
            await self.listener.on_market_data(update['market_data'])

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

         elif 'load_balance' in update:
            await self._call_listener_cb(self.listener.on_balance_update, update['load_balance'])

         elif 'subscribe_dealer_offers' in update:
            sub_reply = update['subscribe_dealer_offers']
            if sub_reply['success'] != True:
               logging.warning(f"failed to subcribe to dealer offers with error: {sub_reply['error']}")

         elif 'dealer_offers' in update:
            dealer_offers = DealerOffers(update['dealer_offers'])
            await self._call_listener_method('on_dealer_offers', dealer_offers)

         elif 'market_order' in update:
            order_reply = update['market_order']
            reference = order_reply['reference']
            if reference in self._requests_cb:
               cb = self._requests_cb.pop(reference)
               await self._call_listener_cb(cb, order_reply)

         elif 'product_fee' in update:
            fee_reply = update['product_fee']
            reference = fee_reply['reference']
            if reference in self._requests_cb:
               cb = self._requests_cb.pop(reference)
               await self._call_listener_cb(cb, fee_reply)

         elif 'authorize' in update:
            if not update['authorize']['success']:
               raise Exception('Failed to renew session token')
         elif 'logout' in update:
            raise Exception('ERROR: we got a logout message. Closing connection')
         else:
            logging.warning('!!! Ignore update\n{} !!!'.format(update))

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
