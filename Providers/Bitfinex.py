import logging
import asyncio

from Factories.Provider.Factory import Factory
from Factories.Definitions import ProviderException, \
   AggregationOrderBook, PositionsReport, BalanceReport, \
   PriceEvent

from Providers.bfxapi.bfxapi import Client
from Providers.bfxapi.bfxapi import Order
import Providers.bfxapi.bfxapi.models as bfx_models

BFX_USD_NET = 'USD Net'
BFX_USD_TOTAL = 'USD total'
BFX_DERIVATIVES_WALLET = 'margin'
BFX_DEPOSIT_METHOD = 'TETHERUSL'

BALANCE_TOTAL = 'total'
BALANCE_FREE = 'free'
BALANCE_RESERVED = 'reserved'

################################################################################
class BitfinexException(Exception):
   pass

################################################################################
class BfxPosition(object):
   def __init__(self, position):
      self.position = position

   def __str__(self):
      lev = self.position.leverage
      if isinstance(lev, float):
         lev = round(lev, 2)

      liq = self.position.liquidation_price
      if isinstance(liq, float):
         liq = round(liq, 2)

      collateral = self.position.collateral
      if collateral != None:
         collateral = round(collateral, 2)

      text = "<id: {} -- vol: {}, price: {} -- lev: {}, liq: {}, col: {}>"
      return text.format(self.position.id, self.position.amount,
         round(self.position.base_price, 2), lev, liq, collateral)

   def __eq__(self, obj):
      if self.position.amount != obj.position.amount:
         return False

      if self.position.base_price != obj.position.base_price:
         return False

      if self.position.leverage != obj.position.leverage:
         return False

      if self.position.liquidation_price != obj.position.liquidation_price:
         return False

      if self.position.collateral != obj.position.collateral:
         return False

      return True

################################################################################
class BfxPositionsReport(PositionsReport):
   def __init__(self, provider):
      super().__init__(provider)
      self.product = provider.product

      #convert position to BfxPosition
      self.positions = {}
      for symbol in provider.positions:
         self.positions[symbol] = {}
         for id in provider.positions[symbol]:
            self.positions[symbol][id] = BfxPosition(provider.positions[symbol][id])

   def __str__(self):
      #header
      result = "  * {} -- exp: {} -- product: {} *\n".format(
         self.name, self.netExposure, self.product)

      #positions
      if not self.product in self.positions:
         result += "    N/A\n"
         return result

      productPos = self.positions[self.product]
      for pos in productPos:
         result += "    {}\n".format(str(productPos[pos]))

      #untracked products
      untrackedProducts = []
      for prod in self.positions:
         if prod != self.product:
            untrackedProducts.append(prod)

      if len(untrackedProducts) != 0:
         result += "\n  + positions for untracked products +\n"
         for prod in untrackedProducts:
            result += "    - {} -".format(prod)
            productPos = self.positions[prod]
            for pos in productPos:
               result += "      {}\n".format(str(productPos[pos]))

      return result

   def __eq__(self, obj):
      if not super().__eq__(obj):
         return False

      if self.product not in self.positions or \
         self.product not in obj.positions:
         return False

      slfPos = self.positions[self.product]
      objPos = obj.positions[self.product]

      if slfPos.keys() != objPos.keys():
         return False

      for id in slfPos:
         if slfPos[id] != objPos[id]:
            return False

      return True

   def getPnl(self):
      if not self.product in self.positions:
         return "N/A"

      if len(self.positions[self.product]) != 1:
         return "N/A"

      id = next(iter(self.positions[self.product]))
      pnl = self.positions[self.product][id].position.profit_loss
      if pnl == None:
         return "N/A"
      return round(pnl, 6)

################################################################################
class BfxBalanceReport(BalanceReport):
   def __init__(self, provider):
      super().__init__(provider)
      self.ccy = provider.derivatives_currency
      self.balances = provider.balances

   def __str__(self):
      mainAcc = {}
      mainCcy = {}
      if BFX_DERIVATIVES_WALLET in self.balances:
         mainAcc = self.balances[BFX_DERIVATIVES_WALLET]

      if self.ccy in mainAcc:
         mainCcy = mainAcc[self.ccy]

      #header
      result = "  + {} +\n".format(self.name)

      mainTotal = "N/A"
      if BALANCE_TOTAL in mainCcy:
         mainTotal = mainCcy[BALANCE_TOTAL]

      mainFree = "N/A"
      if BALANCE_FREE in mainCcy:
         mainFree = mainCcy[BALANCE_FREE]

      #main {account:ccy}
      result += "    * Derivatives Account ({})*\n".format(BFX_DERIVATIVES_WALLET)
      result += "      <[{}] total: {}, free: {}>\n".format(
         self.ccy, mainTotal, mainFree)

      #alt ccy in main acc
      miscCcy = []
      for ccy in mainAcc:
         if ccy != self.ccy:
            miscCcy.append(ccy)

      if len(miscCcy) != 0:
         #header
         result += "\n      - misc currencies -\n"

         #body
         for ccyKey in miscCcy:
            ccy = mainAcc[ccyKey]
            mainTotal = "N/A"
            if BALANCE_TOTAL in ccy:
               mainTotal = ccy[BALANCE_TOTAL]

            mainFree = "N/A"
            if BALANCE_FREE in ccy:
               mainFree = ccy[BALANCE_FREE]

            result += "        <[{}] total: {}, free: {}>\n".format(
               ccyKey, mainTotal, mainFree)

      return result

   def __eq__(self, obj):
      if not super().__eq__(obj):
         return False

      if not BFX_DERIVATIVES_WALLET in obj.balances or \
         not BFX_DERIVATIVES_WALLET in self.balances:
         return False

      wltSelf = self.balances[BFX_DERIVATIVES_WALLET]
      wltObj = obj.balances[BFX_DERIVATIVES_WALLET]

      if wltSelf.keys() != wltObj.keys():
         return False

      return wltSelf == wltObj

################################################################################
class BitfinexProvider(Factory):
   required_settings = {
      'bitfinex': [
         'api_key', 'api_secret',
         'orderbook_product',
         'derivatives_currency',
         'futures_hedging_product',
         'collateral_pct',
         'max_collateral_deviation'
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
      self.indexPrice = 0

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
      self.collateral_pct = self.config['collateral_pct']
      self.max_collateral_deviation = self.config['max_collateral_deviation']
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
      self.connection.ws.on('status_update', self.on_status_update)

   async def loadAddresses(self, callback):
      try:
         deposit_address = await self.connection.rest.get_wallet_deposit_address(
            wallet=BFX_DERIVATIVES_WALLET, method=BFX_DEPOSIT_METHOD)
         self.chainAddresses.set_deposit_address(deposit_address.notify_info.address)
         await callback()
      except Exception as e:
         logging.error(f'Failed to load Bitfinex deposit address: {str(e)}')

   async def loadWithdrawals(self, callback):
      await callback()

   #############################################################################
   #### events
   #############################################################################

   ## connection events ##
   async def on_authenticated(self, auth_message):
      await super().setConnected(True)

      # subscribe to order book
      await self.connection.ws.subscribe('book', self.orderbook_product,
         len=self.order_book_len, prec=self.order_book_aggregation)

      # subscribe to status report
      await self.connection.ws.subscribe_derivative_status(self.product)

   ## balance events ##
   async def on_balance_updated(self, data):
      '''
      This balance update has little use and only serves as a
      vague indicative value. We trigger specific notifications
      on updates to the bfx reserved wallet names instead.
      '''
      pass

   def _explicitly_reset_derivatives_wallet(self):
      balances = {}

      balances[BALANCE_TOTAL] = 0
      balances[BALANCE_FREE] = 0
      balances[BALANCE_RESERVED] = 0

      self.balances[BFX_DERIVATIVES_WALLET] = {}
      self.balances[BFX_DERIVATIVES_WALLET][self.derivatives_currency] = balances

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

      balances[BALANCE_TOTAL] = total_balance
      if free_balance != None:
         balances[BALANCE_FREE] = free_balance
         balances[BALANCE_RESERVED] = reserved_balance

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
      position = bfx_models.Position.from_raw_rest_position(data[2])
      del self.positions[position.symbol][position.id]
      await super().onPositionUpdate()

   async def update_position(self, posObj):
      if posObj.symbol not in self.positions:
         self.positions[posObj.symbol] = {}
      self.positions[posObj.symbol][posObj.id] = posObj
      await super().onPositionUpdate()

   ## margin events ##
   async def on_margin_info_update(self, data):
      logging.info(f'======= on_bitfinex_margin_info_update: {data}')

   ## status event ##
   async def on_status_update(self, status):
      if status == None or 'deriv_price' not in status:
         return
      self.indexPrice = status['deriv_price']
      await self.dealerCallback(self, PriceEvent)

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
         return None
      balance = self.balances[BFX_DERIVATIVES_WALLET][self.derivatives_currency]
      #TODO: account for exposure that can be freed from current orders

      priceBid = self.order_book.get_aggregated_bid_price(self.max_offer_volume)
      priceAsk = self.order_book.get_aggregated_ask_price(self.max_offer_volume)

      if priceBid == None or priceAsk == None:
         return None

      collateralPct = self.getCollateralRatio()
      if balance[BALANCE_FREE] == None or priceAsk.price == None:
         logging.error(f"invalid data: bal: {balance[BALANCE_FREE]}, "\
            f" col_rt: {collateralPct}, price: {priceAsk.price}")
         return None

      result = {}
      result["ask"] = balance[BALANCE_FREE] / (collateralPct * priceAsk.price)
      result["bid"] = balance[BALANCE_FREE] / (collateralPct * priceBid.price)
      return result

   ## exposure ##
   def getExposure(self):
      if not self.isReady():
         return None

      if self.product not in self.positions:
         return 0
      exposure = 0
      for id in self.positions[self.product]:
         exposure += self.positions[self.product][id].amount
      return exposure

   async def updateExposure(self, quantity):
      await self.connection.ws.submit_order(symbol=self.product,
         leverage=self.leverage,
         price=None, # this is a market order, price is ignored
         amount=quantity,
         market_type=bfx_models.order.OrderType.MARKET)

   def getPositions(self):
      return BfxPositionsReport(self)

   ## balance ##
   def getBalance(self):
      return BfxBalanceReport(self)

   def getOpenPrice(self):
      if self.product not in self.positions:
         return None
      if len( self.positions[self.product]) != 1:
         return None

      id = next(iter(self.positions[self.product]))
      return self.positions[self.product][id].base_price

   ## collateral ##
   async def checkCollateral(self, openPrice):
      '''
      We want the liquidation price for our position to be in the
      vicinity of openPrice * (1 - collateralRatio)

      The goal is to keep this provider's liquidation price within
      a specific range of the counterparty's liquidation price. Since
      we can only affect collateral for the position, we first
      establish the price swing from our positions to the target
      liq_price then deduce the collateral amount.

      openPrice comes from the counterparty, collateralRatio comes
      from settings
      '''

      if openPrice == None:
         return

      if self.product not in self.positions:
         return

      if len(self.positions[self.product]) != 1:
         logging.warn(f"{self.name} provider expected 1 position for" \
            f" product {self.product}, got {len(self.positions[self.product])}")
         return

      key = next(iter(self.positions[self.product]))
      position = self.positions[self.product][key]
      if position.liquidation_price == None or position.amount == 0:
         #liquidation price isn't documented right away, may have to wait
         #a little before we can calculate collateral retargetting
         return

      #We want to cover a X% price swing from the open price. Check that
      #the liquidation price on our position is within that margin
      liqPct = abs(position.liquidation_price - openPrice) / openPrice
      collateralPct = self.getCollateralRatio()
      if abs(liqPct - collateralPct) * 100 < self.max_collateral_deviation:
         return None

      #compute the target liquidation price based on openPrice
      swing = openPrice * collateralPct
      if position.amount > 0:
         swing *= -1
      targetLiqPrice = openPrice + swing

      #figure out the swing vs our position's price
      totalSwing = position.base_price - targetLiqPrice
      if position.amount < 0:
         totalSwing *= -1

      collateralTarget = position.collateral_min
      if totalSwing > 0:
         collateralTarget = max(collateralTarget, totalSwing * abs(position.amount))

      #if the collateralTarget is within 10% of the minimum allowed
      #collateral value, we ignore it
      if position.collateral / position.collateral_min <= 1.1:
         return

      await self.connection.rest.set_derivative_collateral(
         symbol=self.product, collateral=collateralTarget)

   #############################################################################
   #### state
   #############################################################################
   async def evaluateReadyState(self):
      currentReadyState = super().isReady()
      if self.lastReadyState == currentReadyState:
         return

      self.lastReadyState = currentReadyState
      await super().onReady()
