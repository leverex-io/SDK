import logging
import asyncio
import Factories.Definitions as Definitions
from decimal import Decimal

################################################################################
class CashOpsManager(object):
   def __init__(self, provider):
      #provider is of Factory type
      self.provider = provider
      self.queue = {}
      self.counter = 0

   def addTask(self, cashOp):
      #set id and increment counter
      cashOp.setId(self.counter)
      self.counter += 1
      self.queue[cashOp.id()] = cashOp
      return cashOp

   def isDone(self, id):
      if id in self.queue:
         return self.queue[id].done()

      #missing id that is less than the counter means
      #the task was completed and cleaned up
      return id < self.counter

   async def process(self):
      hasTasks = len(self.queue) != 0
      while True:
         if not self.queue:
            if hasTasks:
               await self.provider.onBalanceUpdate()
            return

         #select first task in the queue
         key = next(iter(self.queue))
         task = self.queue[key]

         #progress it
         await task.process(self.provider)

         #if it's not completed, return
         if not task.done():
            return

         #delete the task, iterate over next one
         if key in self.queue:
            del self.queue[key]

   def hasTasks(self, taskType=Definitions.CashOperation):
      for taskId in self.queue:
         if isinstance(self.queue[taskId], taskType):
            return True
      return False

   def peekLastTask(self):
      if not self.queue:
         return None

      key = next(reversed(self.queue.keys()))
      return self.queue[key]

   def __str__(self):
      taskId = "N/A"
      if self.queue:
         taskId = next(iter(self.queue))
      result = " |  + {} - current task: #{}\n".format(self.provider.name, taskId)

      if not self.queue:
         result += " |  N/A\n"
      else:
         for key in self.queue:
            cashOp = self.queue[key]
            result += " |     - {}".format(str(cashOp))

      return result

################################################################################
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
      self.cashOps = CashOpsManager(self)

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
         return Decimal(self.collateral_pct / 100)
      return Decimal(1 / self.leverage)

   ## initialization events ##
   async def setConnected(self, value):
      self._connected = value
      await self.onReady()

   async def setInitBalance(self):
      if self._balanceInitialized:
         raise Definitions.ProviderException("init failure")
      self._balanceInitialized = True

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
      #progress current cash operations
      await self.cashOps.process()

      #then propagate the notification to rest of dealer
      await self.dealerCallback(self, Definitions.Balance)

   async def onTransactionUpdate(self):
      await self.dealerCallback(self, Definitions.Transaction)

   async def onOrderBookUpdate(self):
      await self.dealerCallback(self, Definitions.OrderBook)

   ## methods ##
   async def updateExposure(self, exposure):
      #set exposure on service
      #typically handled by the taker, as a consequence of
      #maker position events
      pass

   async def withdraw(self, amount, callback):
      logging.debug("[withdraw]")

   def getPendingWithdrawals(self):
      pass

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

   def withdrawalsLoaded(self):
      return False

   def getCashMetrics(self):
      return None

   def getCashOpsStr(self):
      return str(self.cashOps)
