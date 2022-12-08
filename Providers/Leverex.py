import logging
import asyncio
import json

from Factories.Provider.Factory import Factory
from Factories.Definitions import ProviderException, Position, PositionsReport
from .leverex_core.api_connection import AsyncApiConnection, SessionInfo
from .leverex_core.product_mapping import get_product_info

################################################################################
class LeverexException(Exception):
   pass

################################################################################
class LeverexPositionsReport(PositionsReport):
   def __init__(self, provider):
      super().__init__(provider)
      self.session = provider.currentSession
      self.indexPrice = provider.indexPrice
      self.positions = provider.orders

      #set index price for orders, it will update pnl
      for pos in self.positions:
         self.positions[pos].setIndexPrice(self.indexPrice)

   def __str__(self):
      result = ""

      #header
      result += "  * {} -- exp: {}".format(self.name, self.netExposure)

      if self.session is not None and self.session.isOpen():
         result +=" -- session: {}, open price: {}".format(
            self.session.getSessionId(), self.session.getOpenPrice())

      result += " -- index price: {} *\n".format(self.indexPrice)

      #positions
      for pos in self.positions:
         result += "    {}\n".format(str(self.positions[pos]))

      return result

   def __eq__(self, obj):
      if not super().__eq__(obj):
         return False

      return self.positions.keys() == obj.positions.keys()

################################################################################
class LeverexProvider(Factory):
   required_settings = {
      'leverex': [
         'api_endpoint',
         'login_endpoint',
         'key_file_path',
         'email',
         'product'
      ]
   }

   ## setup ##
   def __init__(self, config):
      super().__init__("Leverex")
      self.config = config
      self.connection = None
      self.balances = {}

      self.netExposure = 0
      self.orders = {}
      self.currentSession = None
      self.lastReadyState = False
      self.indexPrice = None

      #check for required config entries
      for k in self.required_settings:
         if k not in self.config:
            raise LeverexException(f'Missing \"{k}"\ in config')

         for kk in self.required_settings[k]:
            if kk not in self.config[k]:
               raise LeverexException(f'Missing \"{kk}\" in config group \"{k}\"')

      self.product = self.config['leverex']['product']
      productInfo = get_product_info(self.product)
      self.ccy = productInfo.cash_ccy

   ##
   def setup(self, callback):
      super().setup(callback)

      #setup leverex connection
      leverexConfig = self.config['leverex']
      self.connection = AsyncApiConnection(
         customer_email=leverexConfig['email'],
         api_endpoint=leverexConfig['api_endpoint'],
         login_endpoint=leverexConfig['login_endpoint'],
         key_file_path=leverexConfig['key_file_path'],
         dump_communication=False)

   ##
   def getAsyncIOTask(self):
      return asyncio.create_task(self.connection.run(self))

   #############################################################################
   #### notifications
   #############################################################################

   ## connection status events ##
   def on_connected(self):
      pass

   async def on_authorized(self):
      await super().setConnected(True)

      async def balanceCallback(balances):
         await self.onLoadBalance(balances)
         await self.setInitBalance()
         await self.evaluateReadyState()
      self.connection.loadBalances(balanceCallback)

      await self.connection.load_open_positions(
         target_product=self.product, callback=self.on_positions_loaded)
      await self.connection.subscribe_session_open(self.product)
      await self.connection.subscribe_to_product(self.product)
      #await self._leverex_connection.load_deposit_address(callback=self.on_leverex_deposit_address_loaded)
      #await self._leverex_connection.load_whitelisted_addresses(callback=self.on_leverex_addresses_loaded)

   ## balance events ##
   async def onLoadBalance(self, balances):
      for balance_info in balances:
         self.balances[balance_info['currency']] = float(balance_info['balance'])

      await super().onBalanceUpdate()

   ## position events ##
   async def on_positions_loaded(self, orders):
      def getId(order):
         return order.id
      orders.sort(key=getId)

      for order in orders:
         self.storeActiveOrder(order)

      await super().setInitPosition()
      await self.evaluateReadyState()

   async def on_order_created(self, order):
      self.storeActiveOrder(order)
      await super().onPositionUpdate()

   async def on_order_filled(self, order):
      logging.warning("++++++++++ [Leverex.on_order_filled] implement me")

   ## session notifications
   async def on_session_open(self, sessionInfo):
      self.currentSession = SessionInfo(sessionInfo)
      for orderId in self.orders:
         self.orders[orderId].setSessionIM(self.currentSession)
      await self.evaluateReadyState()

   async def on_session_closed(self, sessionInfo):
      self.currentSession = SessionInfo(sessionInfo)
      await self.evaluateReadyState()

   def on_market_data(self, marketData):
      self.indexPrice = float(marketData['live_cutoff'])

   #############################################################################
   #### methods
   #############################################################################

   ## state ##
   def isReady(self):
      return self.lastReadyState

   async def evaluateReadyState(self):
      def assessReadyState():
         if not super(LeverexProvider, self).isReady():
            return False

         #check session is opened
         if self.currentSession == None or not self.currentSession.isOpen():
            return False

         return True

      currentReadyState = assessReadyState()
      if self.lastReadyState == currentReadyState:
         return

      self.lastReadyState = currentReadyState
      await super().onReady()

   ##offers
   def getOpenVolume(self):
      if not self.isReady():
         return None

      leverageRatio = 0.1
      price = self.currentSession.getOpenPrice()
      if self.ccy not in self.balances:
         return None
      balance = self.balances[self.ccy]

      #TODO: account for exposure that can be freed from current orders

      result = {}
      result['ask'] = balance / (leverageRatio * price)
      result['bid'] = balance / (leverageRatio * price)
      return result

   async def submitOffers(self, offers):
      def callback(reply):
         if 'submit_offer' not in reply:
            return
         if 'result' not in reply['submit_offer']:
            return

         if reply['submit_offer']['result'] != 1:
            logging.error(f"Failed to submit offers with error: {str(reply)}")

      await self.connection.submit_offers(
         target_product=self.product, offers=offers, callback=callback)

   ## orders ##
   def storeActiveOrder(self, order):
      if not order.is_trade_position:
         #this is a rolled over position, it carries the full exposure
         #for this session, therefor we reset the net exposure
         self.netExposure = 0

         #it is also the only active order as of this moment,
         #therefor wipe the order map
         self.orders = {}

      order.setSessionIM(self.currentSession)
      self.orders[order.id] = order
      if order.is_sell:
         self.netExposure = self.netExposure - order.quantity
      else:
         self.netExposure = self.netExposure + order.quantity

   def getPositions(self):
      return LeverexPositionsReport(self)

   ## exposure ##
   def getExposure(self):
      if not self.isReady():
         return None
      return round(self.netExposure, 8)

   ## balance ##
   def getBalance(self):
      return self.balances