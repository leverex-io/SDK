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

      self._leverage      = None #in multiples
      self.collateral_pct = None #in pct
      self.openPrice = None
      self.chainAddresses = Definitions.DepositWithdrawAddresses()

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
      #set exposure on service
      #typically handled by the taker, as a consequence of
      #maker position events
      pass

   def withdraw(self, withdrawInfo):
      logging.debug("[withdraw]")

   async def submitOffers(self, offers):
      #push price offers to service
      #typically a maker feature, called from hedger
      pass

   async def checkCollateral(self, openPrice):
      #check and adjust collateral of position if necessary
      #typically handled by the taker, as a consequence of
      #maker position events
      pass

   async def setOpenPrice(self, price):
      self.openPrice = price
      await self.dealerCallback(self, Definitions.Collateral)

   ## getters ##
   def getExposure(self):
      logging.debug("[getPostion]")

   def getOpenVolume(self):
      logging.debug("[getOpenVolume]")

   def getBalance(self):
      logging.debug("[getBalance]")

   def getPositions(self):
      logging.debug("[getPositions]")

   def getOpenPrice(self):
      if not self.isReady():
         return None
      return self.openPrice

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

   def getPendingWithdrawals(self):
      return None

   def getCashMetrics(self):
      return None