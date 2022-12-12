import logging
import asyncio
import json

from Factories.Provider.Factory import Factory
from Factories.Definitions import ProviderException, Position, \
   PositionsReport, BalanceReport, SessionInfo
from .leverex_core.api_connection import AsyncApiConnection, ORDER_ACTION_UPDATED
from .leverex_core.product_mapping import get_product_info

################################################################################
class LeverexException(Exception):
   pass

################################################################################
class SessionOrders(object):
   def __init__(self, sessionId):
      self.id = sessionId
      self.orders = {}
      self.netExposure = 0
      self.session = None

   def setSessionObj(self, sessionObj):
      if sessionObj.getSessionId() != self.id:
         return
      self.session = sessionObj
      for order in self.orders:
         self.orders[order].setSessionIM(self.session)

   def setIndexPrice(self, price):
      for order in self.orders:
         self.orders[order].setIndexPrice(price)

   def setOrder(self, order, eventType):
      #set session IM
      if self.session != None:
         order.setSessionIM(self.session)

      self.orders[order.id] = order
      if order.is_filled() and eventType == ORDER_ACTION_UPDATED:
         #filled orders do not affect exposure, return false
         return False

      vol = order.quantity
      if order.is_sell():
         vol *= -1
      self.netExposure += vol

      #return true if setting this order affected net exposure
      return True

   def getNetExposure(self):
      return round(self.netExposure, 8)

   def getCount(self):
      return len(self.orders)

   def __eq__(self, obj):
      if obj == None:
         return False

      return self.id == obj.id and self.orders.keys() == obj.orders.keys()

################################################################################
class LeverexPositionsReport(PositionsReport):
   def __init__(self, provider):
      super().__init__(provider)

      #get sessionId for current session
      sessionId = None
      if provider.currentSession != None:
         sessionId = provider.currentSession.getSessionId()
      self.indexPrice = provider.indexPrice

      #grab orders for session id
      self.orderData = None
      if sessionId in provider.orderData:
         self.orderData = provider.orderData[sessionId]

         #set index price for orders, it will update pnl
         self.orderData.setIndexPrice(self.indexPrice)

   def __str__(self):
      #header
      pnl = self.getPnl()
      result = "  * {} -- exp: {}, pnl: {}".format(\
         self.name, self.netExposure, pnl)

      #grab session from orderData
      session = None
      if self.orderData != None:
         session = self.orderData.session

      #print session info
      if session is not None and session.isOpen():
         result += " -- session: {}, open price: {}".format(
            session.getSessionId(), session.getOpenPrice())
      result += " -- index price: {} *\n".format(self.indexPrice)

      if self.getOrderCount() == 0:
         result += "    N/A\n"
         return result

      #positions
      orderDict = {
         'ROLL' : [],
         'MAKER' : [],
         'TAKER' : []
      }

      if self.orderData != None:
         for pos in self.orderData.orders:
            order = self.orderData.orders[pos]
            if order.is_trade_position():
               if order.is_taker:
                  orderDict['TAKER'].append(pos)
               else:
                  orderDict['MAKER'].append(pos)
            else:
               orderDict['ROLL'].append(pos)

      for posType in orderDict:
         orderList = orderDict[posType]
         if len(orderList) == 0:
            continue

         result += "    - {} -\n".format(posType)
         for orderId in orderList:
            result += "      {}\n".format(str(self.orderData.orders[orderId]))
         result += "\n"

      return result

   def __eq__(self, obj):
      if not super().__eq__(obj):
         return False
      return self.orderData == obj.orderData

   def getOrderCount(self):
      if self.orderData == None:
         return 0
      return self.orderData.getCount()

   def getPnl(self):
      if self.orderData == None:
         return "N/A"

      pnl = 0
      for orderId in self.orderData.orders:
         orderPL = self.orderData.orders[orderId].trade_pnl
         if orderPL == None:
            return "N/A"
         pnl += orderPL
      return round(pnl, 6)

################################################################################
class LeverexBalanceReport(BalanceReport):
   def __init__(self, provider):
      super().__init__(provider)
      self.balances = provider.balances
      self.ccy = provider.ccy

   def __str__(self):
      #header
      result = "  + {} +\n".format(self.name)

      #breakdown
      for ccy in self.balances:
         result += "    <{}: {}".format(ccy, self.balances[ccy])
         if ccy == self.ccy:
            result += " (total)"
         result += ">\n"

      if len(self.balances) == 0:
         result += "    <N/A>\n"

      return result

   def __eq__(self, obj):
      if not super().__eq__(obj):
         return False

      if self.balances.keys() != obj.balances.keys():
         return False

      for ccy in self.balances:
         if self.balances[ccy] == None:
            return False

         if self.balances[ccy] != obj.balances[ccy]:
            return False

      return True

################################################################################
class LeverexProvider(Factory):
   required_settings = {
      'leverex': [
         'api_endpoint',
         'login_endpoint',
         'key_file_path',
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
      self.orderData = {}
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

      #leverex leverage is locked at 10x
      self.setLeverage(10)

   ##
   def setup(self, callback):
      super().setup(callback)

      #setup leverex connection
      leverexConfig = self.config['leverex']
      self.connection = AsyncApiConnection(
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
      for order in orders:
         self.storeOrder(order, ORDER_ACTION_UPDATED)

      await super().setInitPosition()
      await self.evaluateReadyState()

   async def on_order_event(self, order, eventType):
      if self.storeOrder(order, eventType):
         await super().onPositionUpdate()

   ## session notifications
   async def on_session_open(self, sessionInfo):
      await self.setSession(SessionInfo(sessionInfo))

   async def on_session_closed(self, sessionInfo):
      await self.setSession(SessionInfo(sessionInfo))

   async def setSession(self, session):
      self.currentSession = session
      sessionId = session.getSessionId()
      if sessionId not in self.orderData:
         self.orderData[sessionId] = SessionOrders(sessionId)
      self.orderData[sessionId].setSessionObj(session)
      await self.evaluateReadyState()

   def on_market_data(self, marketData):
      self.indexPrice = float(marketData['live_cutoff'])

   #############################################################################
   #### methods
   #############################################################################

   ## state ##
   def isReady(self):
      return self.lastReadyState

   def isBroken(self):
      if self.currentSession == None:
         return False
      return not self.currentSession.isHealthy()

   def getStatusStr(self):
      if not super().isReady():
         return super().getStatusStr()

      if self.currentSession == None:
         return "missing session data"
      if not self.currentSession.isOpen():
         return "session is closed"
      if not self.currentSession.isHealthy():
         return "session is damaged"

      return "N/A"

   async def evaluateReadyState(self):
      def assessReadyState():
         if not super(LeverexProvider, self).isReady():
            return False

         #check session is opened
         if self.currentSession == None or \
            not self.currentSession.isOpen() or \
            not self.currentSession.isHealthy():
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

      leverageRatio = 1 / self.leverage
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
   def storeOrder(self, order, eventType):
      sessionId = order.session_id
      if sessionId not in self.orderData:
         #create SessionOrders object
         self.orderData[sessionId] = SessionOrders(sessionId)

         #set session object if we have one
         if self.currentSession != None and \
            self.currentSession.getSessionId() == sessionId:
            self.orderData[sessionId].setSessionObj(self.currentSession)

      return self.orderData[sessionId].setOrder(order, eventType)

   def getPositions(self):
      return LeverexPositionsReport(self)

   ## exposure ##
   def getExposure(self):
      if not self.isReady():
         return None

      if self.currentSession == None or not self.currentSession.isOpen():
         return None

      sessionId = self.currentSession.getSessionId() 
      if sessionId not in self.orderData:
         return None

      return self.orderData[sessionId].getNetExposure()

   ## balance ##
   def getBalance(self):
      return LeverexBalanceReport(self)