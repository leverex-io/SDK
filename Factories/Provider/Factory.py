import logging
import asyncio
import Factories.Definitions as Definitions

class Factory(object):
   ## setup ##
   def __init__(self, name):
      self._name = name
      self.dealerCallback = None
      self._connected = False
      self._balanceInitialized = False
      self._positionInitialized = False

      #in multiples
      self._leverage = None

      #in pct
      self.collateral_pct = None

   def setup(self, callback):
      if callback == None:
         raise Definitions.ProviderException("missing hedging callback")
      self.dealerCallback = callback
      if self.leverage == None:
         raise Definitions.ProviderException(\
            f"leverage for provider {self.name} was not set")

   def getAsyncIOTask(self):
      pass

   def setLeverage(self, leverage):
      if self._leverage != None:
         raise Definitions.ProviderException(\
            f"leverage for provider {self.name} was not set")
      self._leverage = leverage

   @property
   def leverage(self):
      return self._leverage

   def getCollateralRatio(self):
      #use leverage if collateral_pct is not explicit
      if self.collateral_pct != None:
         return self.collateral_pct / 100
      return 1 / self.leverage

   ## initialization events ##
   async def setConnected(self, value):
      self._connected = value
      await self.onReady()

   async def setInitBalance(self):
      if self._balanceInitialized:
         raise Definitions.ProviderException("init failure")
      self._balanceInitialized = True
      await self.onBalanceUpdate()

   async def setInitPosition(self):
      if self._positionInitialized:
         raise Definitions.ProviderException("init failure")
      self._positionInitialized = True
      await self.onPositionUpdate()

   ## ready state ##
   def isReady(self):
      return self._connected and \
         self._balanceInitialized and \
         self._positionInitialized

   def isBroken(self):
      #by default, we assume we cannot detect a broken state
      return False

   async def waitOnReady(self):
      while True:
         if self.isReady():
            return
         await asyncio.sleep(0.1)

   def printReadyState(self):
      print (f"----- Provider: {self._name}, ready: {self.isReady()} -----\n"
         f"  connected: {self._connected}, balance init: {self._balanceInitialized}, position init: {self._positionInitialized}")

   ## notifications ##
   async def onReady(self):
      await self.dealerCallback(self, Definitions.Ready)

   def onNewPrice(self, price):
      pass

   def onNewOrder(self, order):
      pass

   async def onPositionUpdate(self):
      await self.dealerCallback(self, Definitions.Position)

   async def onBalanceUpdate(self):
      await self.dealerCallback(self, Definitions.Balance)

   async def onOrderBookUpdate(self):
      await self.dealerCallback(self, Definitions.OrderBook)

   ## methods ##
   async def updateExposure(self, exposure):
      logging.debug("[updateExposure]")

   def withdraw(self, withdrawInfo):
      logging.debug("[withdraw]")

   async def submitOffers(self, offers):
      logging.debug("[submitOffers]")

   ## getters ##
   def getExposure(self):
      logging.debug("[getPostion]")

   def getOpenVolume(self):
      logging.debug("[getOpenVolume]")

   def getBalance(self):
      logging.debug("[getBalance]")

   def getPositions(self):
      logging.debug("[getPositions]")

   def getStatusStr(self):
      if not self.isReady():
         if not self._connected:
            return "awaiting login..."
         if not self._balanceInitialized:
            return "awaiting balance snapshot..."
         if not self._positionInitialized:
            return "awaiting orders snapshot..."

      return "N/A"

   @property
   def name(self):
      return self._name
