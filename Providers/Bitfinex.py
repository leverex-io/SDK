import logging
import asyncio

from Factories.Provider.Factory import Factory
from Factories.Definitions import ProviderException, \
   AggregationOrderBook, PositionsReport, BalanceReport, \
   PriceEvent, CashOperation

from Providers.bfxapi.bfxapi import Client
from Providers.bfxapi.bfxapi import Order
import Providers.bfxapi.bfxapi.models as bfx_models


################################################################################
class BitfinexException(Exception):
   pass

################################################################################
class BfxAccounts():
   DERIVATIVES = 'margin'
   EXCHANGE    = 'exchange'

########
class BfxBalances():
   TOTAL    = 'total'
   FREE     = 'free'
   RESERVED = 'reserved'

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

########
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
      result = " ** {} -- exp: {} -- product: {}\n".format(
         self.name, self.netExposure, self.product)

      #positions
      if not self.product in self.positions:
         result += "    N/A\n"
         return result

      productPos = self.positions[self.product]
      for pos in productPos:
         result += " *  {}\n".format(str(productPos[pos]))

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
      self.ccy = [provider.ccy, provider.ccy_base]
      self.acc = [BfxAccounts.DERIVATIVES, BfxAccounts.EXCHANGE]
      self.balances = provider.balances

   def __str__(self):
      #header
      result = " +- {}:\n".format(self.name)

      hasAcc = False
      for i in range(0, len(self.acc)):
         acc = self.acc[i]
         if not acc in self.balances:
            continue
         hasAcc = True
         result += " +--- Account: {}\n".format(acc)

         accDict = self.balances[acc]
         hasCcy = False
         for ccy in accDict:
            if ccy not in accDict:
               continue
            hasCcy = True

            mainTotal = "N/A"
            if BfxBalances.TOTAL in accDict[ccy]:
               mainTotal = round(accDict[ccy][BfxBalances.TOTAL], 2)

            mainFree = "N/A"
            if BfxBalances.FREE in accDict[ccy]:
               mainFree = round(accDict[ccy][BfxBalances.FREE], 2)

            result += " +    <[{}] total: {}, free: {}>\n".format(
               ccy, mainTotal, mainFree
            )

         if not hasCcy:
            result += " +    <N/A>\n"
         elif i < len(self.acc) - 1:
            result += " +\n"

      if not hasAcc:
            result += " +  <N/A>\n"
      return result

   def __eq__(self, obj):
      if not super().__eq__(obj):
         return False

      if not BfxAccounts.DERIVATIVES in obj.balances or \
         not BfxAccounts.DERIVATIVES in self.balances:
         return False

      wltSelf = self.balances[BfxAccounts.DERIVATIVES]
      wltObj = obj.balances[BfxAccounts.DERIVATIVES]

      if wltSelf.keys() != wltObj.keys():
         return False

      return wltSelf == wltObj

################################################################################
class BfxBalanceSwap(CashOperation):
   def __init__(self, accFrom, accTo, ccyFrom, ccyTo, amount = None):
      super().__init__()
      self.accFrom = accFrom
      self.accTo = accTo
      self.ccyFrom = ccyFrom
      self.ccyTo = ccyTo
      self.amount = amount
      self.baseAmount = 0

   async def doTheTask(self, bfx):
      if self.amount == None:
         #no amount was mentionned, move it all
         if self.accFrom not in bfx.balances or \
            self.ccyFrom not in bfx.balances[self.accFrom]:
            self.state = CashOperation.DONE
            return

         bal = bfx.balances[self.accFrom][self.ccyFrom]
         self.amount = bal[BfxBalances.TOTAL]

      if self.amount == 0:
         self.state = CashOperation.DONE
         return

      #get base amount
      if self.accTo in bfx.balances and self.ccyTo in bfx.balances[self.accTo]:
         self.baseAmount = bfx.balances[self.accTo][self.ccyTo][BfxBalances.TOTAL]

      await bfx.connection.rest.submit_wallet_transfer(
         self.accFrom, self.accTo,
         self.ccyFrom, self.ccyTo,
         self.amount
      )

   def assessProgress(self, bfx):
      if self.accTo not in bfx.balances:
         return False

      bal = None
      if self.accTo in bfx.balances and self.ccyTo in bfx.balances[self.accTo]:
         bal = bfx.balances[self.accTo][self.ccyTo]
      if bal == None:
         return False
      return bal[BfxBalances.TOTAL] >= self.baseAmount + self.amount


#######
class BfxWithdrawal(CashOperation):
   def __init__(self, amount, callback):
      super().__init__()
      self.amount = amount
      self.positionId = None
      self.callback = callback

   async def doTheTask(self, bfx):
      await bfx.connection.rest.submit_wallet_withdraw(
         wallet=BfxAccounts.EXCHANGE,
         method=bfx.deposit_method,
         amount=self.amount,
         address=bfx.chainAddresses.getDefaultWithdrawAddr()
      )
      if self.callback != None:
         await self.callback()

   def assessProgress(self, bfx):
      #should prompt bfx by positionId instead
      if BfxAccounts.EXCHANGE not in bfx.balances:
         return False

      bal = bfx.balances[BfxAccounts.EXCHANGE][bfx.ccy_base]
      return bal[BfxBalances.TOTAL] == 0

########
class BfxCancelWithdrawals(CashOperation):
   def __init__(self):
      super().__init__()
      self.state = CashOperation.DONE

   ##
   async def doTheTask(self, bfx):
      #TODO: cancel ongoing "movements"
      return

   ##
   def assessProgress(self, bfx):
      #TODO: assess state of cancelled "movements"
      return True


################################################################################
class BitfinexProvider(Factory):
   required_settings = {
      'bitfinex': [
         'api_key', 'api_secret',
         'base_currency',
         'derivatives_currency',
         'product',
         'collateral_pct',
         'max_collateral_deviation',
         'deposit_method'
      ],
      'hedger': [
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
      for k in self.required_settings:
         if k not in config:
            raise BitfinexException(f'Missing \"{k}\" in config')

         for kk in self.required_settings[k]:
            if kk not in config[k]:
               raise BitfinexException(f'Missing \"{kk}\" in config group \"{k}\"')

      self.config = config['bitfinex']
      self.ccy_base = self.config['base_currency']
      self.ccy = self.config['derivatives_currency']
      self.product = self.config['product']
      self.collateral_pct = self.config['collateral_pct']
      self.max_collateral_deviation = self.config['max_collateral_deviation']
      self.max_offer_volume = config['hedger']['max_offer_volume']
      self.deposit_method = self.config['deposit_method']

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
            wallet=BfxAccounts.DERIVATIVES, method=self.deposit_method)
         self.chainAddresses.setDepositAddr(deposit_address.notify_info.address)
         await callback()
      except Exception as e:
         logging.error(f'Failed to load Bitfinex deposit address: {str(e)}')

   def setWithdrawAddresses(self, addresses):
      self.chainAddresses.setWithdrawAddresses(addresses)

   async def loadWithdrawals(self, callback):
      await callback()

   #############################################################################
   #### events
   #############################################################################

   ## connection events ##
   async def on_authenticated(self, auth_message):
      await super().setConnected(True)

      # subscribe to order book
      await self.connection.ws.subscribe('book', self.product,
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

      balances[BfxBalances.TOTAL] = 0
      balances[BfxBalances.FREE] = 0
      balances[BfxBalances.RESERVED] = 0

      self.balances[BfxAccounts.DERIVATIVES] = {}
      self.balances[BfxAccounts.DERIVATIVES][self.ccy] = balances

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

      balances[BfxBalances.TOTAL] = total_balance
      if free_balance != None:
         balances[BfxBalances.FREE] = free_balance
         balances[BfxBalances.RESERVED] = reserved_balance

      self.balances[wallet.type][wallet.currency] = balances
      if wallet.type in [BfxAccounts.DERIVATIVES, BfxAccounts.EXCHANGE]:
         #check for pending balances
         pendingBal = self.getPendingBalances()
         for acc in pendingBal:
            for ccy in pendingBal[acc]:
               '''
               There is cash in our tracked accounts, under our tracked
               currencies. We want all this cash to be in our derivatives
               account, under our derivative currency. Therefor, we create
               a balance swap task for each of these <account, ccy> pairs
               for the cash operation manager to handle.

               These tasks will not conflict with rebalance cash operations,
               as they are consumed in order of appearance by the cashOps
               manager; i.e. if a rebalance withdrawal triggers this code
               to queue a swap, likely the final swap will have no effect
               as the rebalance withdrawal wiped the pending balance clean
               '''
               self.cashOps.addTask(BfxBalanceSwap(
                  acc, BfxAccounts.DERIVATIVES, ccy, self.ccy))

         await self.onBalanceUpdate()

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

      if BfxAccounts.DERIVATIVES not in self.balances or \
         self.ccy not in self.balances[BfxAccounts.DERIVATIVES]:
         return None
      balance = self.balances[BfxAccounts.DERIVATIVES][self.ccy]
      priceBid = self.order_book.get_aggregated_bid_price(self.max_offer_volume)
      priceAsk = self.order_book.get_aggregated_ask_price(self.max_offer_volume)

      if priceBid == None or priceAsk == None:
         return None

      freeMargin = 0
      balanceKey = BfxBalances.FREE
      if BfxBalances.FREE not in balance:
         balanceKey = BfxBalances.TOTAL
      else:
         #calculate freeable margin
         if self.product in self.positions:
            for id in self.positions[self.product]:
               if self.positions[self.product][id].collateral == None:
                  freeMargin = 0
                  break
               freeMargin += self.positions[self.product][id].collateral

      collateralPct = self.getCollateralRatio()
      if balance[balanceKey] == None or priceAsk.price == None:
         logging.error(f"invalid data: bal: {balance[balanceKey]}, "\
            f" col_rt: {collateralPct}, price: {priceAsk.price}")
         return None

      if balance[balanceKey] < 0:
         #finex balance can be left negative after a forced liquidation
         return None

      askBal = balance[balanceKey]
      bidBal = balance[balanceKey]
      if self.getExposure() > 0:
         askBal += freeMargin*2
      else:
         bidBal += freeMargin*2

      result = {}
      result["ask"] = askBal / (collateralPct * priceAsk.price)
      result["bid"] = bidBal / (collateralPct * priceBid.price)
      return result

   ## cash metrics
   def getCashMetrics(self):
      if BfxAccounts.DERIVATIVES not in self.balances or \
         self.ccy not in self.balances[BfxAccounts.DERIVATIVES]:
         return None
      balance = self.balances[BfxAccounts.DERIVATIVES][self.ccy]
      if not BfxBalances.TOTAL in balance:
         return None

      pending = 0
      pendingDict = self.getPendingBalances()
      for acc in pendingDict:
         for ccy in pendingDict[acc]:
            pending += pendingDict[acc][ccy]

      return {
         'total' : balance[BfxBalances.TOTAL],
         'pending' : pending,
         'ratio' : self.getCollateralRatio()
      }

   ##
   def getPendingBalances(self):
      result = {}
      if BfxAccounts.DERIVATIVES in self.balances:
         bal = self.balances[BfxAccounts.DERIVATIVES]
         if self.ccy_base in bal:
            result[BfxAccounts.DERIVATIVES] = {
               self.ccy_base : bal[self.ccy_base][BfxBalances.TOTAL]
            }

      if BfxAccounts.EXCHANGE in self.balances:
         bal = self.balances[BfxAccounts.EXCHANGE]
         result[BfxAccounts.EXCHANGE] = {}
         if self.ccy_base in bal:
            result[BfxAccounts.EXCHANGE][self.ccy_base] = bal[self.ccy_base][BfxBalances.TOTAL]

         if self.ccy in bal:
            result[BfxAccounts.EXCHANGE][self.ccy] = bal[self.ccy][BfxBalances.TOTAL]

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

   ## withdrawals ##
   async def withdraw(self, amount, callback):
      self.cashOps.addTask(BfxBalanceSwap(
         BfxAccounts.DERIVATIVES, BfxAccounts.EXCHANGE, \
         self.ccy, self.ccy_base, amount))
      task = self.cashOps.addTask(BfxWithdrawal(amount, callback))
      await self.cashOps.process()
      return task

   async def cancelWithdrawals(self):
      self.cashOps.addTask(BfxCancelWithdrawals())
      task = self.cashOps.addTask(BfxBalanceSwap(
         BfxAccounts.EXCHANGE, BfxAccounts.DERIVATIVES, \
         self.ccy_base, self.ccy))
      await self.cashOps.process()
      return task

   def withdrawalsLoaded(self):
      return True

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
