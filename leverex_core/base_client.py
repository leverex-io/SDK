from .utils import LeverexException, SessionInfo, get_product_info, \
   SessionOrders, getBalancesFromJson, ORDER_ACTION_UPDATED, round_down
from .api_connection import AuthApiConnection
from Factories.Definitions import checkConfig

################################################################################
class LeverexBaseClient(object):
   required_settings = {
   'leverex': [
      'api_endpoint',
      'login_endpoint',
      'product'
      ]
   }

   ## setup ##
   def __init__(self, config):
      self.config = config

      #check for required config entries
      checkConfig(self.config, self.required_settings)

      self.connection = None
      self.balances = {}

      self.product = self.config['leverex']['product']
      productInfo = get_product_info(self.product)
      self.ccy = productInfo.cash_ccy
      self.margin_ccy = productInfo.margin_ccy

      self.orderData = {}
      self.currentSession = None
      self.indexPrice = None
      self.withdrawalHistory = None
      self.netExposure = 0
      self.bands = {}

   def setupConnection(self):
      leverexConfig = self.config['leverex']
      keyPath = None
      if 'key_file_path' in leverexConfig:
         keyPath = leverexConfig['key_file_path']

      aeid_endpoint = None
      if 'aeid' in self.config and 'endpoint' in self.config['aeid']:
         aeid_endpoint = self.config['aeid']['endpoint']

      self.connection = AuthApiConnection(
         api_endpoint=leverexConfig['api_endpoint'],
         login_endpoint=leverexConfig['login_endpoint'],
         key_file_path=keyPath,
         dump_communication=False,
         aeid_endpoint=aeid_endpoint)

   async def subscribeToInitialData(self):
      await self.connection.subscribe_to_balance_updates(self.product)
      await self.connection.load_open_positions(
         target_product=self.product,
         callback=self.on_positions_loaded)

   async def subscribeToProductData(self):
      await self.connection.subscribe_session_open(self.product)
      await self.connection.subscribe_to_product(self.product)

   ####
   async def loadAddresses(self, callback=None):
      async def depositAddressCallback(address):
         self.chainAddresses.setDepositAddr(address)
         if callback:
            await callback()

      async def withdrawAddressCallback(addresses):
         addressList = []
         for addr in addresses:
            addressList.append(addr)
         self.chainAddresses.setWithdrawAddresses(addressList)
         if callback:
            await callback()

      await self.connection.load_deposit_address(depositAddressCallback)
      await self.connection.load_whitelisted_addresses(withdrawAddressCallback)

   ## listeners ##
   async def on_balance_update(self, balances):
      self.balances = getBalancesFromJson(balances)

   async def on_positions_loaded(self, orders):
      for order in orders:
         self.storeOrder(order, ORDER_ACTION_UPDATED)

   async def on_session_open(self, sessionInfo):
      await self.setSession(SessionInfo(sessionInfo))

   async def on_session_closed(self, sessionInfo):
      await self.setSession(SessionInfo(sessionInfo))

   async def on_market_data(self, marketData):
      self.indexPrice = float(marketData['live_cutoff'])

   ## session methods ##
   async def setSession(self, session):
      self.currentSession = session
      sessionId = session.getSessionId()
      if sessionId not in self.orderData:
         self.orderData[sessionId] = SessionOrders(sessionId)
      self.orderData[sessionId].setSessionObj(session)

   def getExposure(self):
      if self.currentSession == None or not self.currentSession.isOpen():
         return None

      sessionId = self.currentSession.getSessionId() 
      if sessionId not in self.orderData:
         return None

      return self.orderData[sessionId].getNetExposure()

   def getTotalPnl(self):
      orders = self.getSessionOrders()
      totalPnl = 0
      for orderId in orders:
         order = orders[orderId]
         order.setIndexPrice(self.indexPrice)
         order.computePnL()
         totalPnl += order.trade_pnl
      return round_down(totalPnl, 6)

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

   ## getters ##
   def getSessionOrders(self):
      currentSessionId = None
      if self.currentSession:
         currentSessionId = self.currentSession.getSessionId()
      if not currentSessionId:
         raise Exception()

      sessionOrders = self.orderData[currentSessionId]
      if not sessionOrders:
         raise Exception()
      return sessionOrders.orders
