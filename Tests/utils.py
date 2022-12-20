import asyncio

from Factories.Provider.Factory import Factory
from Factories.Definitions import AggregationOrderBook

price = 10000

####### test providers
class TestProvider(Factory):
   def __init__(self, name, startBalance=0):
      super().__init__(name)

      self.startBalance = startBalance
      self.balance = 0
      self.explicitState = True
      self.withdrawalHist = None

   def getAsyncIOTask(self):
      return asyncio.create_task(self.bootstrap())

   async def bootstrap(self):
      await super().setConnected(True)
      self.balance = self.startBalance
      await super().setInitBalance()

   async def updateBalance(self, balance):
      self.balance = balance
      await super().onBalanceUpdate()

   async def initPositions(self):
      await super().setInitPosition()

   def getOpenVolume(self):
      if not self.isReady():
         return None

      vol = self.balance / (price * self.getCollateralRatio())
      exposure = self.getExposure()
      bid = vol - exposure
      ask = vol + exposure
      return { 'ask' : ask, 'bid' : bid }

   def getCashMetrics(self):
      return {
         'total' : self.balance,
         'pending' : 0,
         'ratio' : self.getCollateralRatio(),
         'price' : price
      }

   def isReady(self):
      if self.explicitState == True:
         return super().isReady()
      else:
         return self.explicitState

   async def setExplicitState(self, state):
      self.explicitState = state
      await super().onReady()

   async def checkCollateral(self, openPrice):
      self.targetCollateral = None

      if openPrice == None:
         return

      if not self.isReady():
         return

      exposure = abs(self.getExposure())
      if exposure == None or exposure == 0:
         return

      self.targetCollateral = exposure * self.getCollateralRatio() * openPrice

   async def loadAddresses(self, callback):
      self.chainAddresses.setDepositAddress("abcd")
      await callback()

   async def loadWithdrawals(self, callback):
      self.withdrawalHist = []
      await callback()

   def getPendingWithdrawals(self):
      return self.withdrawalHist

########
class TestMaker(TestProvider):
   def __init__(self, startBalance=0, startPositions=[]):
      super().__init__("TestMaker", startBalance)

      self.startPositions = startPositions
      self.offers = []
      self.orders = []
      self.brokenState = False
      self.setLeverage(10)
      self.targetCollateral = None

   async def bootstrap(self):
      await super().bootstrap()
      await self.initPositions(self.startPositions)
      await self.setOpenPrice(price)

   async def initPositions(self, startPositions):
      self.orders.extend(startPositions)
      await super().initPositions()

   async def submitOffers(self, offers):
      self.offers.append(offers)

   async def newOrder(self, order):
      self.orders.append(order)
      await super().onPositionUpdate()

   def getExposure(self):
      if not super().isReady():
         return None

      exposure = 0
      for order in self.orders:
         orderQ = order.quantity
         if order.is_sell():
            orderQ *= -1
         exposure += orderQ

      return round(exposure, 8)

   async def explicitBreak(self):
      self.brokenState = True
      await self.setExplicitState(False)

   def isBroken(self):
      return self.brokenState

########
def getOrderBookSnapshot(volume):
   orders = []
   vol = volume / 2
   for i in range(0, 5):
      spread = 20*vol
      orders.append([price + spread, 1, -vol]) #ask
      orders.append([price - spread, 1,  vol]) #bid
      vol = vol / 2
   return orders

########
class TestTaker(TestProvider):
   def __init__(self, startBalance=0, startExposure=0):
      super().__init__("TestTaker", startBalance)

      self.startExposure = startExposure
      self.order_book = AggregationOrderBook()
      self.exposure = 0
      self.collateral_pct = 15

   async def bootstrap(self):
      await super().bootstrap()
      await self.initExposure(self.startExposure)

   async def initExposure(self, startExposure):
      self.exposure = startExposure
      await super().initPositions()

   async def populateOrderBook(self, volume):
      self.order_book.reset()
      orders = getOrderBookSnapshot(volume)
      self.order_book.setup_from_snapshot(orders)
      await super().onOrderBookUpdate()

   def getExposure(self):
      if not super().isReady():
         return None
      return self.exposure

   async def updateExposure(self, exposure):
      self.exposure += exposure
      await super().onPositionUpdate()

   async def loadAddresses(self, callback):
      await callback()
