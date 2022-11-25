import logging
import asyncio

import Factories.Definitions as Definitions

class DealerException(Exception):
   pass

class DealerFactory(object):
   def __init__(self, maker, taker, hedgingStrat):
      self.maker = maker         #Provider
      self.taker = taker         #Provider
      self.hedger = hedgingStrat #HedgerFactory

   async def run(self):
      #sanity checks
      if self.hedger == None:
         raise DealerException("[DealerFactory::run] missing hedging strat")

      self.maker.setup(self.onEvent)
      tasks = [self.maker.getAsyncIOTask()]

      self.taker.setup(self.onEvent)
      tasks.append(self.taker.getAsyncIOTask())

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

      else:
         logging.debug(f"[onMakerEvent] ignoring event {eventType}")

   ## taker ##
   async def onTakerEvent(self, eventType):
      if eventType == Definitions.Position:
         #taker positions changed, sanity check vs maker positions
         await self.hedger.onTakerPositionEvent(self.maker, self.taker)

      elif eventType == Definitions.OrderBook:
         #taker order book update, recompute offers accordingly
         await self.hedger.onTakerOrderBookEvent(self.maker, self.taker)

      else:
         logging.debug(f"[onTakerEvent] ignoring event {eventType}")

   ## balance ##
   async def onBalanceEvent(self):
      await self.hedger.onBalanceEvent(self.maker, self.taker)

   ## ready ##
   async def onReadyEvent(self):
      await self.hedger.onReadyEvent(self.maker, self.taker)