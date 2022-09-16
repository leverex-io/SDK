import argparse
import asyncio
import json
import logging
import sys
import time

import uvicorn
from fastapi import FastAPI
from fastapi.responses import HTMLResponse
from fastapi.middleware.cors import CORSMiddleware

from bfxapi import Client
from bfxapi import Order as BitfinexOrder
from bfxapi.models import Position

sys.path.append('..')

from trader_core.api_connection import AsyncApiConnection, PriceOffer, WithdrawInfo
from trader_core.product_mapping import get_product_info

from bitfinx_order_book import AggregationOrderBook


class TransferInfo():
   def __init__(self, *, from_wallet, to_wallet, from_currency, to_currency, amount):
      self._from_wallet = from_wallet
      self._to_wallet = to_wallet
      self._from_currency = from_currency
      self._to_currency = to_currency
      self._amount = amount

   def __str__(self):
      return f'{self._amount} {self._from_wallet}_{self._from_currency} to {self._to_wallet}_{self._to_currency}'


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


class HedgingDealer():
   def __init__(self, configuration):
      self.hedging_settings = configuration['hedging_settings']

      self._overseer_mode = False
      if 'overseer_mode' in self.hedging_settings:
         self._overseer_mode = bool(self.hedging_settings['overseer_mode'])

      # setup Bitfinex connection
      self.bitfinex_book = AggregationOrderBook()

      self.bitfinex_config = configuration['bitfinex']

      self.bitfinex_balances = None
      self.bitfinex_orderbook_product = self.hedging_settings['bitfinex_orderbook_product']
      self.bitfinex_derivatives_currency = self.hedging_settings['bitfinex_derivatives_currency']
      self.bitfinex_futures_hedging_product = self.hedging_settings['bitfinex_futures_hedging_product']
      self.min_bitfinex_leverage = self.hedging_settings['min_bitfinex_leverage']
      self.bitfinex_leverage = self.hedging_settings['bitfinex_leverage']
      self.max_bitfinex_leverage = self.hedging_settings['max_bitfinex_leverage']
      self.price_ratio = self.hedging_settings['price_ratio']
      self.leverex_product = self.hedging_settings['leverex_product']
      self.max_leverex_offer_volume = self.hedging_settings['max_offer_volume']

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

      self._bfx = Client(API_KEY=self.bitfinex_config['api_key'],
                         API_SECRET=self.bitfinex_config['api_secret'],
                         logLevel=bitfinex_log_level)

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

      # setup Leverex connection
      self._leverex_config = configuration['leverex']
      self._leverex_connection = AsyncApiConnection(customer_email=self._leverex_config['email'],
                                                    api_endpoint=self._leverex_config['api_endpoint'],
                                                    login_endpoint=self._leverex_config['login_endpoint'],
                                                    key_file_path=self._leverex_config['key_file_path'],)

      self.leverex_balances = {}
      self._bitfinex_balances = {}

      self._app = FastAPI()

      origins = [
          '*'
      ]

      self._app.add_middleware(CORSMiddleware,
                               allow_origins=origins,
                               allow_credentials=True,
                               allow_methods=["*"],
                               allow_headers=["*"])

      self._app.get('/')(self.report_api_entry)
      self._app.get('/api/balance')(self.report_balance)

      # DEV REST endpoints. should be disabled
      # self._app.get('/api/rebalance')(self._rebalance_if_required)
      # self._app.get('/api/complete_transfer')(self._complete_transfer)

      self._app.get('/api/rebalance_address_info')(self.report_rebalance_address_info)
      self._app.get('/api/rebalance_state')(self.report_rebalance_state)

      self._app.get('/api/leverex/session_info')(self.report_session_info)
      self._app.get('/api/leverex/deposits')(self.report_deposits)
      self._app.get('/api/leverex/withdrawals')(self.report_withdrawals)

      self._app.get('/api/bitfinex/position')(self.report_bitfinex_position)

      config = uvicorn.Config(self._app, host='0.0.0.0', port=configuration['status_server']['port'], log_level="debug")
      self._status_server = uvicorn.Server(config)

      self._positions = {}
      self._net_exposure = 0.0
      self._current_session_info = None

      self._bitfinex_position_loaded = False
      self._leverex_orders_loaded = False

      self._bitfinex_positions = {self.bitfinex_futures_hedging_product: None}

      self._bitfinex_deposit_addresses = None
      self._leverex_deposit_addresses = None
      self._rebalance_enabled = False
      self._rebalance_disable_reason = "Address info not loaded"

      rebalance_settings = configuration['rebalance_settings']

      # force_rebalance_disabled - master key to completely disable any rebalance or cash transfer
      self._force_rebalance_disabled = rebalance_settings.get('force_rebalance_disabled', False)

      self._rebalance_method = rebalance_settings['bitfinex_method']
      self._rebalance_threshold = float(rebalance_settings['rebalance_threshold'])
      self._bitfinex_rebalance_currency = rebalance_settings['bitfinex_rebalance_currency']
      self._tarnsfer_funds_threshold = rebalance_settings['tarnsfer_funds_threshold']

      # all funds detected here should be moved to derivatives account.
      # leverex deposits go here
      self._bitfinex_deposit_wallet = 'margin'
      # all funds moved here should be withdrawn to leverex address
      self._bitfinex_withdraw_wallet = 'exchange'

      # rebalance state variables
      self._rebalance_in_progress = False
      self._withdraw_amount = False

      self._bitfinex_withdraw_scheduled = False
      self._bitfinex_withdraw_requested = False
      self._leverex_withdraw_scheduled = False

      # there could be only one transfer from wallet to wallet on Bitfinex platform
      self._current_bitfinex_transfer = None

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

      if len(self._leverex_deposit_addresses.get_deposit_address()) == 0:
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

      if len(self._bitfinex_deposit_addresses.get_deposit_address()) == 0:
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
               <p>General</p>
               <p>&emsp;<a href="/api/balance">Balances</a></p>
               <p>&emsp;<a href="/api/rebalance_address_info">Info on rebalance related addresses data</a></p>
               <p>&emsp;<a href="/api/rebalance_state">Current rebalance state</a></p>
               <p>Leverex</p>
               <p>&emsp;<a href="/api/leverex/deposits">Deposits</a></p>
               <p>&emsp;<a href="/api/leverex/withdrawals">Withdrawals</a></p>
               <p>&emsp;<a href="/api/leverex/session_info">Current session info</a></p>
               <p>Bitfinex</p>
               <p>&emsp;<a href="/api/bitfinex/position">Current position</a></p>
           </body>
       </html>
       """
      return HTMLResponse(content=html_content, status_code=200)

   async def report_rebalance_state(self):
      return self._get_rebalance_state_report()

   async def report_rebalance_address_info(self):
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

      if self._rebalance_enabled and not self._force_rebalance_disabled:
         result['rebalance state'] = 'Enabled'
      else:
         if self._force_rebalance_disabled:
            result['rebalance state'] = 'Disabled: by force_rebalance_disabled'
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
         if self.bitfinex_derivatives_currency in self._bitfinex_balances['margin']:
            bitfinex_total = float(self._bitfinex_balances['margin'][self.bitfinex_derivatives_currency]['total'])

      if bitfinex_total is not None and leverex_total is not None:
         portfolio = bitfinex_total + leverex_total

      return {'leverex': leverex_balances, 'bitfinex': bitfinex_balances, 'portfolio': portfolio}

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

   async def report_withdrawals(self):
      loop = asyncio.get_running_loop()
      fut = loop.create_future()

      async def cb(withdrawals):
         fut.set_result(withdrawals)

      start_time = time.time()

      await self._leverex_connection.load_withdrawals_history(callback=cb)
      withdrawals = await fut

      end_time = time.time()

      withdrawals_info = [
          {'url': w.unblinded_link,
           'timestamp': str(w.timestamp),
           'tx_id': w.transacion_id,
           'amount': w.amount,
           'state': w.status} for w in withdrawals]

      loading_time = f'{end_time - start_time} seconds'

      return {
          'withdrawals': withdrawals_info,
          'loading_time': loading_time
      }

   async def report_deposits(self):
      loop = asyncio.get_running_loop()
      fut = loop.create_future()

      async def cb(deposits):
         fut.set_result(deposits)

      start_time = time.time()

      await self._leverex_connection.load_deposits_history(callback=cb)
      deposits = await fut

      end_time = time.time()

      deposits_info = [{'url': d.unblinded_link, 'confirmations': d.confirmations_count, 'tx_id': d.transacion_id, 'timestamp': str(d.timestamp)} for d in deposits]

      loading_time = f'{end_time - start_time} seconds'

      return {
          'deposits': deposits_info,
          'loading_time': loading_time
      }

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

      if self._bitfinex_deposit_addresses is None:
         try:
            deposit_address = await self._bfx.rest.get_wallet_deposit_address(wallet=self._bitfinex_deposit_wallet,
                                                                              method=self._rebalance_method)
            self._bitfinex_deposit_addresses = DepositWithdrawAddresses()
            self._bitfinex_deposit_addresses.set_deposit_address(deposit_address.notify_info.address)
            self._validate_rebalance_feature_state()
         except Exception as e:
            logging.error(f'Failed to load Bitfinex deposit address: {str(e)}')

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

   async def on_bitfinex_wallet_snapshot(self, wallets_snapshot):
      # XXX probably I should explicitly set derivatives balance to 0 here
      self._explicitly_reset_derivatives_wallet()

      for wallet in wallets_snapshot:
         await self.on_bitfinex_wallet_update(wallet)

   async def on_bitfinex_wallet_update(self, wallet):
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

      if free_balance is not None:
         await self._complete_transfer()
         await self._rebalance_if_required()

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

      # wallet updates are relatively rear and rebalance might take a lot of time
      # just to speedup things a little bit add rebalance validation here as well
      await self._complete_transfer()
      await self._rebalance_if_required()

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

   # this function should return None, if balance is not loaded
   # 0 balance and not loaded balances are different when this method is used
   def _get_leverex_total(self):
      if self._target_margin_product not in self.leverex_balances:
         return None
      if self._target_ccy_product not in self.leverex_balances:
         return None

      return self._get_buying_power() + self._get_margin_reserved()

   def _get_bitfinex_pending_deposit_balance(self):
      wallets = self._bitfinex_balances.get(self._bitfinex_deposit_wallet, None)
      if wallets is None:
         return None

      balances = wallets.get(self._bitfinex_rebalance_currency, None)
      if balances is None:
         return None

      return balances['free']

   def _get_bitfinex_pending_withdraw_balance(self):
      wallets = self._bitfinex_balances.get(self._bitfinex_withdraw_wallet, None)
      if wallets is None:
         return None

      balances = wallets.get(self._bitfinex_rebalance_currency, None)
      if balances is None:
         return None

      return balances['free']

   # USAGE NOTE: if there is 0 balance on margin - we could never get this balance
   # and None will be always returned.
   # Issue that it could cause - no rebalance ever.
   # Considerations:
   #  - return 0 if there is no such wallet or currency. But it is unclear if it was loaded or not
   #  - always have non-zero balance added manually to a margin.
   def _get_bitfinex_total(self):
      derivatives_wallets = self._bitfinex_balances.get('margin', None)
      if derivatives_wallets is None:
         return None

      balances = derivatives_wallets.get(self.bitfinex_derivatives_currency, None)
      if balances is None:
         return None

      reported_total = balances['total']
      free_balance = balances['free']
      reserved_balance = balances['reserved']

      # NOTE: in case all the cash was removed from here - it will be 0
      # and no update to set free from None to 0 from Bitfinex
      if reported_total == 0:
         return 0

      if free_balance is None or reserved_balance is None:
         return None

      return free_balance + reserved_balance

   def _explicitly_reset_derivatives_wallet(self):
      logging.info('Setting derivatives wallet balance to 0 explicitly')

      balances = {}

      balances['total'] = 0
      balances['free'] = 0
      balances['reserved'] = 0

      self._bitfinex_balances['margin'] = {}

      self._bitfinex_balances['margin'][self.bitfinex_derivatives_currency] = balances

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

      if ask_volume > self.max_leverex_offer_volume:
         ask_volume = self.max_leverex_offer_volume

      if bid_volume > self.max_leverex_offer_volume:
         bid_volume = self.max_leverex_offer_volume

      ask = self.bitfinex_book.get_aggregated_ask_price(ask_volume)
      bid = self.bitfinex_book.get_aggregated_bid_price(bid_volume)

      if ask is not None and bid is not None:
         # if bitfinex could not cover requested volume
         if ask.volume < ask_volume:
            ask_volume = ask.volume
         if bid.volume < bid_volume:
            bid_volume = bid.volume

         ask_price = ask.price * (1 + self.price_ratio)
         bid_price = bid.price * (1 - self.price_ratio)

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

   async def onLoadBalance(self, balances):
      logging.info('Balance loaded {}'.format(balances))
      for balance_info in balances:
         self.leverex_balances[balance_info['currency']] = float(balance_info['balance'])

      await self._rebalance_if_required()

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
      logging.info(f'======== {len(orders)} positions loaded from Leverex')

   def on_leverex_deposit_address_loaded(self, address):
      if self._leverex_deposit_addresses is None:
         self._leverex_deposit_addresses = DepositWithdrawAddresses()

      self._leverex_deposit_addresses.set_deposit_address(address)
      self._validate_rebalance_feature_state()

   def on_leverex_addresses_loaded(self, addresses):
      if self._leverex_deposit_addresses is None:
         self._leverex_deposit_addresses = DepositWithdrawAddresses()

      self._leverex_deposit_addresses.set_withdraw_addresses(addresses)
      self._validate_rebalance_feature_state()

   async def _create_bitfinex_order(self, leverex_order):
      # negative amount is sell, positive is buy
      # we need to invert Leverex side here
      if leverex_order.is_sell:
         quantity = leverex_order.quantity
      else:
         quantity = -leverex_order.quantity

      logging.info(f'Submitting order to Bitfinex {quantity}')

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
         self._net_exposure = 0

      if order.is_sell:
         self._net_exposure = self._net_exposure - order.quantity
      else:
         self._net_exposure = self._net_exposure + order.quantity

      logging.info(f'[store_active_order] Net exposure : {self._net_exposure}')

   # position matched on Leverex
   def on_order_created(self, order):
      if order.product_type == self.leverex_product:
         self.store_active_order(order)

         if not self._overseer_mode:
            if order.is_trade_position:
               # create order on Bitfinex
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

   def _get_rebalance_state_report(self):
      report = {}

      report['in progress'] = self._rebalance_in_progress
      if self._rebalance_in_progress:
         report['scheduled amount'] = self._withdraw_amount

         pending_deposited_balance = self._get_bitfinex_pending_deposit_balance()
         if pending_deposited_balance is None:
            report['pending deposited balance'] = 'None'
         else:
            report['pending deposited balance'] = pending_deposited_balance

         pending_withdraw_balance = self._get_bitfinex_pending_withdraw_balance()
         if pending_withdraw_balance is None:
            report['pending withdraw balance'] = 'None'
         else:
            report['pending withdraw balance'] = pending_withdraw_balance

         if self._bitfinex_withdraw_scheduled:
            report['withdraw from'] = 'Bitfinex'
         elif self._leverex_withdraw_scheduled:
            report['withdraw from'] = 'Leverex'
         else:
            report['withdraw from'] = 'undefinex'

         report['bitfinex withdraw requested'] = self._bitfinex_withdraw_requested

      # validate that Leverex balance is loaded
      total_leverex_balance = self._get_leverex_total()
      if total_leverex_balance is None:
         report['leverex balance'] = 'not available'
      else:
         report['leverex balance'] = total_leverex_balance

      total_bitfinex_balance = self._get_bitfinex_total()
      if total_bitfinex_balance is None:
         report['bitfinex balance'] = 'not available'
      else:
         report['bitfinex balance'] = total_bitfinex_balance

      report['rebalance required'] = False

      if total_leverex_balance is not None and total_bitfinex_balance is not None:
         difference = abs(total_bitfinex_balance - total_leverex_balance)
         relative_diff = difference / (total_bitfinex_balance + total_leverex_balance)

         report['absolute balance diff'] = difference
         report['relative balance diff'] = relative_diff
         report['to transfer'] = round(difference / 2)
         report['relative balance threshold'] = self._rebalance_threshold

         if self._rebalance_threshold < relative_diff and difference > self._tarnsfer_funds_threshold:
            report['rebalance required'] = self._rebalance_enabled
            if self._rebalance_enabled:
               if total_bitfinex_balance > total_leverex_balance:
                  report['rebalance state'] = 'Required from Bitfinex to Leverex'
               else:
                  report['rebalance state'] = 'Required from Leverex to Bitfinex'
            else:
               report['rebalance state'] = 'Disabled'
         else:
            report['rebalance state'] = 'Not required'

      return report

   async def _complete_transfer(self):
      # check that Bitfinex have completed monetary check and transfer wallet and
      # margin wallets are unlocked and have available balance

      derivatives_balance = self._get_bitfinex_total()
      if derivatives_balance is None:
         return

      if self._bitfinex_withdraw_scheduled:
         if self._bitfinex_withdraw_requested:
            # withdraw request already created
            return

         pending_withdraw_balance = self._get_bitfinex_pending_withdraw_balance()
         if pending_withdraw_balance is None:
            logging.debug('BF->LEVEREX: withdraw scheduled. No funds on withdraw wallet')
            return

         # if this is a withdraw from Bitfinex to Leverex - create withdraw request
         if pending_withdraw_balance != self._withdraw_amount:
            logging.error(f'BF->LEVEREX: Unexpected transfer amount: {self._withdraw_amount} is expected, but {pending_withdraw_balance} detected on withdraw wallet ({self._bitfinex_withdraw_wallet})')
            if pending_withdraw_balance < self._withdraw_amount:
               logging.error('BF->LEVEREX: Could not execute withdraw')
               return

         await self._make_bitfinex_withdraw(self._withdraw_amount)
      else:
         if self._leverex_withdraw_scheduled:
            # validate amount
            pending_deposited_balance = self._get_bitfinex_pending_deposit_balance()
            if pending_deposited_balance is None:
               logging.debug('LEVEREX->BF: withdraw scheduled. No funds on deposit wallet yet')
               return

            if pending_deposited_balance != self._withdraw_amount:
               logging.error(f'LEVEREX->BF: Unexpected transfer amount: {self._withdraw_amount} is expected, but {pending_deposited_balance} detected on deposit wallet')
               # not a critical error. cash should be transferred form transfer
               # wallet to a margin wallet

            # put cash to margin wallet
            await self._transfer_from_deposit(pending_deposited_balance)
            # complete rebalance
            logging.info('LEVEREX->BF: Completing rebalance')
            self._rebalance_from_leverex_completed()
         else:
            pending_deposited_balance = self._get_bitfinex_pending_deposit_balance()
            if pending_deposited_balance is not None and pending_deposited_balance > self._tarnsfer_funds_threshold:
               # just transfer cash to a margin wallet
               await self._transfer_from_deposit(pending_deposited_balance)

            pending_withdraw_balance = self._get_bitfinex_pending_withdraw_balance()
            if pending_withdraw_balance is not None and pending_withdraw_balance > self._tarnsfer_funds_threshold:
               logging.info('Return funds from withdraw wallet to a margin wallet')
               await self._transfer_back_from_withdraw(pending_withdraw_balance)

   async def _rebalance_if_required(self):
      if self._force_rebalance_disabled:
         return

      rebalance_report = self._get_rebalance_state_report()
      if rebalance_report['rebalance required']:
         # already marked as in progress
         if rebalance_report['in progress']:
            return

         total_leverex_balance = rebalance_report['leverex balance']
         total_bitfinex_balance = rebalance_report['bitfinex balance']

         rebalance_amount = rebalance_report['to transfer']

         # no need to make a rebalance on a very small amount
         if rebalance_amount < self._tarnsfer_funds_threshold:
            logging.info(f'no rebalance due to a small amount {rebalance_amount}. Controlled by tarnsfer_funds_threshold config')
            return

         if total_bitfinex_balance > total_leverex_balance:
            await self._rebalance_from_bitfinex_to_leverex(rebalance_amount)
         else:
            await self._rebalance_from_leverex_to_bitfinex(rebalance_amount)

   def _start_rebalance(self, amount):
      if self._rebalance_in_progress:
         raise Exception('Rebalance already started')

      self._rebalance_in_progress = True
      self._withdraw_amount = amount

   def _rebalance_completed(self):
      if not self._rebalance_in_progress:
         raise Exception('Rebalance not started')

      self._rebalance_in_progress = False
      self._withdraw_amount = 0

   def _rebalance_from_bitfinex_completed(self):
      if not self._bitfinex_withdraw_scheduled:
         raise Exception('Rebalance from Bitfinex was not started')

      self._rebalance_completed()
      self._bitfinex_withdraw_scheduled = False
      self._bitfinex_withdraw_requested = False

   def _rebalance_from_leverex_completed(self):
      if not self._leverex_withdraw_scheduled:
         raise Exception('Rebalance from Leverex was not started')

      self._rebalance_completed()
      self._leverex_withdraw_scheduled = False

   def _start_rebalance_from_bitfinex(self, amount):
      logging.info(f'Starting withdraw from Bitfinex: {amount}')
      self._start_rebalance(amount)
      self._bitfinex_withdraw_scheduled = True

   def _start_rebalance_from_leverex(self, amount):
      self._start_rebalance(amount)
      self._leverex_withdraw_scheduled = True

   async def _bitfinex_wallet_transfer_with_delay(self, *, from_wallet, to_wallet,
                                                  from_currency, to_currency, amount):
      new_transfer = TransferInfo(from_wallet=from_wallet, to_wallet=to_wallet,
                                  from_currency=from_currency,
                                  to_currency=to_currency, amount=amount)

      lock = asyncio.Lock()

      async with lock:
         if self._current_bitfinex_transfer is not None:
            logging.warning(f'Transfer {str(new_transfer)} rejected. Reason: transfer in progress {str(self._current_bitfinex_transfer)}')
            return False

         self._current_bitfinex_transfer = new_transfer
         logging.info(f'Starting new bitfinex wallet transfer {str(new_transfer)}')

      retry_count = 0

      try:
         while True:
            try:
               await self._bfx.rest.submit_wallet_transfer(to_wallet=to_wallet,
                                                           from_wallet=from_wallet,
                                                           to_currency=to_currency,
                                                           from_currency=from_currency,
                                                           amount=amount)
               break
            except Exception as e:
               retry_count = retry_count + 1
               if retry_count > 5:
                  raise e

               delay = 3 * retry_count

               logging.error(f'Transfer failed on Bitfinex. Retry in {delay} second : {str(e)}')
               await asyncio.sleep(delay)
      finally:
         async with lock:
            self._current_bitfinex_transfer = None
      return True

   async def _transfer_to_withdraw(self, amount):
      await self._bitfinex_wallet_transfer_with_delay(from_wallet='trading',
                                                      to_wallet=self._bitfinex_withdraw_wallet,
                                                      from_currency=self.bitfinex_derivatives_currency,
                                                      to_currency=self._bitfinex_rebalance_currency,
                                                      amount=amount)

   # transfer cash from BF "transfer" wallet to a margin wallet
   async def _transfer_from_deposit(self, amount):
      await self._bitfinex_wallet_transfer_with_delay(to_wallet='trading',
                                                      from_wallet=self._bitfinex_deposit_wallet,
                                                      to_currency=self.bitfinex_derivatives_currency,
                                                      from_currency=self._bitfinex_rebalance_currency,
                                                      amount=amount)

   async def _transfer_back_from_withdraw(self, amount):
      await self._bitfinex_wallet_transfer_with_delay(to_wallet='trading',
                                                      from_wallet=self._bitfinex_withdraw_wallet,
                                                      to_currency=self.bitfinex_derivatives_currency,
                                                      from_currency=self._bitfinex_rebalance_currency,
                                                      amount=amount)

   async def _make_bitfinex_withdraw(self, amount):
      if self._leverex_deposit_addresses is None or self._leverex_deposit_addresses.get_deposit_address() is None:
         logging.error('Leverex deposit address is undefined. Could not rebalance')
         return

      if self._bitfinex_withdraw_requested:
         return

      leverex_deposit_address = self._leverex_deposit_addresses.get_deposit_address()

      decrease_amount = 1
      logging.info(f'Decreasing withdraw amount by {decrease_amount}')
      amount = amount - decrease_amount
      logging.info(f'Submitting Bitfinex withdraw request for {amount} from {self._bitfinex_withdraw_wallet} via {self._rebalance_method} to {leverex_deposit_address}')

      result = await self._bfx.rest.submit_wallet_withdraw(wallet=self._bitfinex_withdraw_wallet,
                                                           method=self._rebalance_method,
                                                           amount=amount,
                                                           address=leverex_deposit_address)
      print(f'Result: {str(result.notify_info)}')

      if result.notify_info.id != 0:
         self._bitfinex_withdraw_requested = True
         logging.info(f'Withdraw created : {result.notify_info.id}')
      else:
         logging.error('Withdraw not created')

   # deposit update from leverex
   # pending rebalance from Bitfinex to Leverex could be completed if deposit is confirmed
   async def on_deposit_update(self, deposit_info):
      logging.info(f'Leverex deposit detected: {deposit_info.confirmations_count} confirmations. URL: {deposit_info.unblinded_link}')
      if deposit_info.confirmations_count == 3:
         if self._bitfinex_withdraw_scheduled:
            logging.info('Deposit confirmed. Completing rebalance.')
            self._rebalance_from_bitfinex_completed()

   # withdraw update from leverex
   async def on_withdraw_update(self, withdraw_info):
      pass

   async def _rebalance_from_bitfinex_to_leverex(self, amount):
      self._start_rebalance_from_bitfinex(amount)

      # transfer to withdraw wallet
      try:
         # actual withdraw request will be created right after balance update received
         await self._transfer_to_withdraw(amount)
      except Exception:
         logging.exception(f'Failed to transfer {amount} to exchange wallet {self.bitfinex_derivatives_currency}')

   async def _rebalance_from_leverex_to_bitfinex(self, amount):
      self._start_rebalance_from_leverex(amount)

      # use forced address, even if it is not whitelisted
      bitfinex_deposit_address = None
      if self._bitfinex_deposit_addresses is None or self._bitfinex_deposit_addresses.get_deposit_address() is None:
         logging.error('Bitfinex deposit address is not loaded. Could not rebalance')
         return

      bitfinex_deposit_address = self._bitfinex_deposit_addresses.get_deposit_address()
      if len(bitfinex_deposit_address) == 0:
         logging.error('Bitfinex deposit address is an empty string. Could not rebalance')
         return

      # address whitelisting validation
      if self._leverex_deposit_addresses is None or self._leverex_deposit_addresses.get_withdraw_addresses() is None:
         logging.error('Leverex whitelisted addresses list is not loaded. Could not rebalance')
         return

      if bitfinex_deposit_address not in self._leverex_deposit_addresses.get_withdraw_addresses():
         logging.error('Bitfinex deposit address is not whitelisted. Could not rebalance')
         return

      logging.info(f'Creating rebalance withdraw from Leverex to address {bitfinex_deposit_address}')
      await self._leverex_connection.withdraw_liquid(address=bitfinex_deposit_address,
                                                     currency='USDT',
                                                     amount=amount,
                                                     callback=None)
      # withdraw progress will be completed once we got cash on funding account
      # in case of manual testing please make sure to transfer it accordingly
      # XXX: what about fee?

   async def on_withdraw_request_response(self, withdraw_info):
      if withdraw_info.status_code == WithdrawInfo.WITHDRAW_FAILED:
         logging.error(f'Withdraw request failed on Leverex: {withdraw_info.error_message}')
      else:
         logging.info('Leverex withdraw created')

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
       'status_server': ['port'],
       'leverex': ['api_endpoint', 'login_endpoint', 'key_file_path', 'email'],
       'bitfinex': ['api_key', 'api_secret'],
       'rebalance_settings': ['bitfinex_method', 'bitfinex_rebalance_currency', 'rebalance_threshold', 'tarnsfer_funds_threshold'],
       'hedging_settings': ['leverex_product',
                            'bitfinex_futures_hedging_product',
                            'bitfinex_orderbook_product',
                            'bitfinex_derivatives_currency',
                            'price_ratio',
                            'min_bitfinex_leverage',
                            'bitfinex_leverage',
                            'max_bitfinex_leverage',
                            'max_offer_volume']
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
