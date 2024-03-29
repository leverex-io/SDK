import asyncio
import random
import string
from decimal import Decimal

from Factories.Provider.Factory import Factory
from Factories.Definitions import AggregationOrderBook, \
   CashOperation, OpenVolume, TheTxTracker
from leverex_core.utils import WithdrawInfo, round_down

price = 10000

####### test providers
class TestProvider(Factory):
   def __init__(self, name, startBalance=0, pendingWithdrawals=None):
      super().__init__(name)

      self.startBalance = Decimal(startBalance)
      self.balance = Decimal(0)
      self.explicitState = True
      self.withdrawalsToPush = []
      self.withdrawalHist = None
      self.cancelWithdrawalsRequested = None

      if pendingWithdrawals == None:
         return

      for amount in pendingWithdrawals:
         self.withdrawalHist = []
         self.withdrawalHist.append({
            'amount': amount,
            'status': WithdrawInfo.WITHDRAW_PENDING
         })

   def getAsyncIOTask(self):
      return asyncio.create_task(self.bootstrap())

   async def bootstrap(self):
      await super().setConnected(True)
      self.balance = self.startBalance
      await super().setInitBalance()

   async def updateBalance(self, balance):
      self.balance = Decimal(balance)
      await super().onBalanceUpdate()

   async def initPositions(self):
      await super().setInitPosition()

   def getOpenVolume(self):
      if not self.isReady():
         return None

      exposure = self.getExposure() * price * self.getCollateralRatio()
      balance = self.balance - abs(exposure)

      bidExposure = 0
      askExposure = 0
      if exposure > 0:
         bidExposure = exposure
      else:
         askExposure = abs(exposure)

      return OpenVolume(balance,
         askExposure, price * self.getCollateralRatio(),
         bidExposure, price * self.getCollateralRatio()
      )

   def getCashMetrics(self):
      pending = 0
      for wtdr in self.withdrawalHist:
         if wtdr['status'] == WithdrawInfo.WITHDRAW_PENDING:
            pending += wtdr['amount']

      return {
         'total' : self.balance,
         'pending' : pending,
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

      self.targetCollateral = exposure \
         * self.getCollateralRatio() \
         * openPrice

   async def loadAddresses(self, callback):
      self.chainAddresses.setDepositAddr("abcd")
      self.chainAddresses.setWithdrawAddresses(["efgh", "ijkl"])
      await callback()

   async def loadWithdrawals(self, callback):
      if self.withdrawalHist == None:
         self.withdrawalHist = []
      await callback()

   def withdrawalsLoaded(self):
      return self.withdrawalHist is not None

   async def withdraw(self, amount, callback):
      self.withdrawalsToPush.append([amount, callback,
         CashOperation()])
      return self.withdrawalsToPush[-1][-1]

   async def pushWithdrawal(self):
      if self.withdrawalHist == None:
         raise Exception("withdrawals arent ready")

      for wtd in self.withdrawalsToPush:
         #set in history
         wtd[2].state = CashOperation.MONITORING_TASK
         self.withdrawalHist.append({
               'amount': wtd[0],
               'status': WithdrawInfo.WITHDRAW_BROADCASTED,
               'task': wtd[2]
            })
         #callback
         if wtd[1] != None:
            await wtd[1]()

         #update balances
         await self.updateBalance(self.balance - wtd[0])

      self.withdrawalsToPush.clear()

   async def completeWithdrawal(self, counterparty):
      for wtd in self.withdrawalHist:
         if wtd['status'] != WithdrawInfo.WITHDRAW_BROADCASTED:
            continue

         wtd['status'] = WithdrawInfo.WITHDRAW_COMPLETED
         wtd['task'].state = CashOperation.DONE
         await counterparty.updateBalance(counterparty.balance + wtd['amount'])

   async def completeTransaction(self, amount):
      txid = ''.join(random.choice(string.ascii_lowercase) for i in range(12))
      TheTxTracker.addTransaction(txid,
         self.chainAddresses.getDepositAddr(),
         2, [{
            'currency': 'USDT',
            'amount': amount
      }])
      await self.updateBalance(self.balance + amount)
      await self.onTransactionUpdate()

   async def cancelWithdrawals(self):
      if self.cancelWithdrawalsRequested != None:
         raise Exception("cancellation already underway")
      self.cancelWithdrawalsRequested = CashOperation()
      return self.cancelWithdrawalsRequested

   async def completeWithdrawCancellation(self):
      if self.cancelWithdrawalsRequested == None:
         raise Exception("cancel withdrawals was not requested")

      self.cancelWithdrawalsRequested.state = CashOperation.DONE
      amount = 0
      for wtdr in self.withdrawalHist:
         if wtdr['status'] == WithdrawInfo.WITHDRAW_PENDING:
            wtdr['status'] = WithdrawInfo.WITHDRAW_CANCELLED
            amount += wtdr['amount']

      await self.updateBalance(self.balance + amount)
      self.cancelWithdrawalsRequested = None

########
class TestMaker(TestProvider):
   def __init__(self, startBalance=0, startPositions=[], pendingWithdrawals=None):
      super().__init__("TestMaker", startBalance, pendingWithdrawals)

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

   async def submitPrices(self, offers):
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

      return round_down(exposure, 8)

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
class FakeBfxWSConnection(object):
   def __init__(self, provider):
      self.provider = provider

   async def submit_order(self, symbol, leverage, price, amount, market_type):
      self.provider.exposure += amount

class FakeBfxConnection(object):
   def __init__(self, provider):
      self.provider = provider
      self.ws = FakeBfxWSConnection(self.provider)

class TestTaker(TestProvider):
   def __init__(self, startBalance=0, startExposure=0, pendingWithdrawals=None, addr=None):
      super().__init__("TestTaker", startBalance, pendingWithdrawals)

      self.startExposure = startExposure
      self.order_book = AggregationOrderBook()
      self.exposure = Decimal(0)
      self.collateral_pct = 15
      self.setLeverage(100/self.collateral_pct)
      self.addr = addr
      self.connection = FakeBfxConnection(self)
      self.product = "product"

   async def bootstrap(self):
      await super().bootstrap()
      await self.initExposure(self.startExposure)

   async def initExposure(self, startExposure):
      self.exposure = Decimal(startExposure)
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
      self.exposure = Decimal(exposure)
      await super().onPositionUpdate()

   async def loadAddresses(self, callback):
      if self.addr != None:
         self.chainAddresses.setDepositAddr(self.addr)
      await callback()
