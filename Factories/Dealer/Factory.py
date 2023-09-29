import logging
import asyncio

import Factories.Definitions as Definitions
from Factories.StatusReporter.Factory import Factory

class DealerException(Exception):
   pass

class DealerFactory(object):
   def __init__(self, maker, taker, hedgingStrat, statusReporters=[]):
      self.maker = maker         #Provider
      self.taker = taker         #Provider
      self.hedger = hedgingStrat #HedgerFactory
      self.statusReporters = statusReporters
      self._name = "Dealer"

   async def run(self):
      #sanity checks
      try:
         if self.hedger == None:
            raise DealerException("[DealerFactory::run] missing hedging strat")

         ## maker setup ##
         self.maker.setup(self.onEvent)
         tasks = [self.maker.getAsyncIOTask()]

         ## taker setup ##
         self.taker.setup(self.onEvent)
         tasks.append(self.taker.getAsyncIOTask())

         ## hedger setup ##
         self.hedger.setup(self.onEvent)

         ## status reporters setup ##
         for reporter in self.statusReporters:
            reporterTask = reporter.getAsyncIOTask()
            if reporterTask == None:
               continue
            tasks.append(reporterTask)

         #start asyncio tasks
         done, pending = await asyncio.wait(tasks, return_when=asyncio.FIRST_COMPLETED)
      except Exception as e:
         print (f"dealer loop exception: {e}")
         loop = asyncio.get_running_loop()
         loop.stop()
         return

   def stop(self):
      pass

   @property
   def name(self):
      return self._name

   #### events ####
   async def onEvent(self, provider, eventType):
      if eventType == Definitions.Ready:
         #a provider ready state changed
         await self.onReadyEvent()
         return
      elif eventType == Definitions.Balance:
         #balances changed, check for rebalance condition
         await self.onBalanceEvent()
         return
      elif eventType == Definitions.Collateral:
         await self.onCollateralEvent()
         return
      elif eventType == Definitions.PriceEvent:
         await self.onPriceEvent()
         return
      elif eventType == Definitions.Rebalance:
         await self.onRebalanceEvent()
         return
      elif eventType == Definitions.Transaction:
         await self.onTransactionEvent()
         return

      if provider == self.maker:
         await self.onMakerEvent(eventType)
      elif provider == self.taker:
         await self.onTakerEvent(eventType)
      else:
         logging.warn("[onEvent] unexpected provider")

   ## maker ##
   async def onMakerEvent(self, eventType):
      if eventType == Definitions.Position:
         #maker positions changed, update taker positions accordingly
         await self.hedger.onMakerPositionEvent(self.maker, self.taker)
         for reporter in self.statusReporters:
            await reporter.onPositionEvent(self)

      else:
         logging.debug(f"[onMakerEvent] ignoring event {eventType}")

   ## taker ##
   async def onTakerEvent(self, eventType):
      if eventType == Definitions.Position:
         #taker positions changed, sanity check vs maker positions
         await self.hedger.onTakerPositionEvent(self.maker, self.taker)
         await self.onCollateralEvent()
         for reporter in self.statusReporters:
            await reporter.onPositionEvent(self)

      elif eventType == Definitions.OrderBook:
         #taker order book update, recompute offers accordingly
         await self.hedger.onTakerOrderBookEvent(self.maker, self.taker)

      else:
         logging.debug(f"[onTakerEvent] ignoring event {eventType}")

   ## balance ##
   async def onBalanceEvent(self):
      await self.hedger.onBalanceEvent(self.maker, self.taker)
      for reporter in self.statusReporters:
         await reporter.onBalanceEvent(self)

   ## status ##
   async def onReadyEvent(self):
      await self.hedger.onReadyEvent(self.maker, self.taker)
      await self.taker.checkCollateral(self.maker.getOpenPrice())
      for reporter in self.statusReporters:
         await reporter.onReadyEvent(self)

   def isReady(self):
      return self.maker.isReady() \
         and self.taker.isReady() \
         and self.hedger.isReady()

   async def waitOnReady(self):
      await self.maker.waitOnReady()
      await self.taker.waitOnReady()
      await self.hedger.waitOnReady()

      await self.onReadyEvent()

   def getStatusStr(self):
      if not self.taker.isReady():
         return f"{self.taker.name} is not ready"
      if not self.maker.isReady():
         return f"{self.maker.name} is not ready"
      if not self.hedger.isReady():
         return f"{self.hedger.name} is not ready"

   ## collateral ##
   async def onCollateralEvent(self):
      await self.taker.checkCollateral(self.maker.getOpenPrice())

   ## price ##
   async def onPriceEvent(self):
      for reporter in self.statusReporters:
         await reporter.onPriceEvent(self)

   ## rebalance ##
   async def onRebalanceEvent(self):
      for reporter in self.statusReporters:
         await reporter.onRebalanceEvent(self)

   ## transactions ##
   async def onTransactionEvent(self):
      await self.maker.cashOps.process()
      await self.taker.cashOps.process()
      await self.hedger.onBalanceEvent(self.maker, self.taker)
