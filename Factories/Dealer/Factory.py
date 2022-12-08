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

   async def run(self):
      #sanity checks
      if self.hedger == None:
         raise DealerException("[DealerFactory::run] missing hedging strat")

      #maker init task
      self.maker.setup(self.onEvent)
      tasks = [self.maker.getAsyncIOTask()]

      #taker init task
      self.taker.setup(self.onEvent)
      tasks.append(self.taker.getAsyncIOTask())

      #status reporters init task
      for reporter in self.statusReporters:
         reporterTask = reporter.getAsyncIOTask()
         if reporterTask == None:
            continue
         tasks.append(reporterTask)

      #start asyncio loops
      done, pending = await asyncio.wait(tasks, return_when=asyncio.FIRST_COMPLETED)

   def stop(self):
      pass

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

   ## ready ##
   async def onReadyEvent(self):
      await self.hedger.onReadyEvent(self.maker, self.taker)
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