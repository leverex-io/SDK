import logging
import asyncio
import json

from Factories.Provider.Factory import Factory
from Factories.Definitions import ProviderException, Position
from .leverex_core.api_connection import AsyncApiConnection, SessionInfo

################################################################################
class LeverexException(Exception):
   pass

################################################################################
class LeverexProvider(Factory):
   required_settings = {
      'leverex': ['api_endpoint', 'login_endpoint', 'key_file_path', 'email'],
      'hedging_settings' : ['leverex_product']
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

      #check for required config entries
      for k in self.required_settings:
         if k not in self.config:
            raise LeverexException(f'Missing "\{k}"\ in config')

         for kk in self.required_settings[k]:
            if kk not in self.config[k]:
               raise LeverexException(f'Missing "\{kk}\" in config group \"{k}\"')

      self.product = self.config['hedging_settings']['leverex_product']

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
         dump_communication=True)

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
      logging.info('================= Authenticated to Leverex')
      await super().setConnected(True)

      async def balanceCallback(balances):
         await self.setInitBalance()
         await self.onLoadBalance(balances)
      self.connection.loadBalances(balanceCallback)

      await self.connection.load_open_positions(
         target_product=self.product, callback=self.on_positions_loaded)
      await self.connection.subscribe_session_open(self.product)
      #await self._leverex_connection.load_deposit_address(callback=self.on_leverex_deposit_address_loaded)
      #await self._leverex_connection.load_whitelisted_addresses(callback=self.on_leverex_addresses_loaded)

   ## balance events ##
   async def onLoadBalance(self, balances):
      logging.info('Balance loaded {}'.format(balances))
      for balance_info in balances:
         self.balances[balance_info['currency']] = float(balance_info['balance'])

      await super().onBalanceUpdate()
      await self.evaluateReadyState()

   ## position events ##
   async def on_positions_loaded(self, orders):
      await super().setInitPosition()

      #TODO: order timestamp and/or orderId. If the session has a roll,
      #it should ALWAYS be the first trade in the list

      for order in orders:
         self.storeActiveOrder(order)
      logging.info(f'======== {len(orders)} positions loaded from Leverex')
      await super().onPositionUpdate()
      await self.evaluateReadyState()

   async def on_order_created(self, order):
      self.storeActiveOrder(order)
      logging.info(f'======== matched in Leverex for order {str(order)}')
      await super().onPositionUpdate()

   async def on_order_filled(self, order):
      logging.warning("++++++++++ [Leverex.on_order_filled] implement me")

   ## session notifications
   async def on_session_open(self, sessionInfo):
      self.currentSession = SessionInfo(sessionInfo)
      await self.evaluateReadyState()

   async def on_session_closed(self, sessionInfo):
      self.currentSession = SessionInfo(sessionInfo)
      await self.evaluateReadyState()

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
      if self.product not in self.balances:
         logging.error("Missing balances from Leverex!")
         return None
      balance = self.balances[self.product]

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
         # this position is rollover position
         # and it carries full exposure for a session
         self.netExposure = 0

         # it is also the only active order as of this moment
         # therefor wipe the order map
         self.orders = {}

      self.orders[order.id] = order
      if order.is_sell:
         self.netExposure = self.netExposure - order.quantity
      else:
         self.netExposure = self.netExposure + order.quantity

      logging.info(f'[store_active_order] Net exposure : {self.netExposure}')

   ## exposure ##
   def getExposure(self):
      if not self.isReady():
         return None
      return self.netExposure