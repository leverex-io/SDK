import logging
import asyncio

class HedgerFactory(object):
   def __init__(self, name):
      self._name = name
      self._ready = False

   ## ready ##
   async def onReadyEvent(self, maker, taker):
      logging.debug("[HedgerFactory::onReadyEvent]")

   @property
   def name(self):
      return self._name

   def isReady(self):
      return self._ready

   def setReady(self):
      if not self.isReady():
         self._ready = True

   async def waitOnReady(self):
      while True:
         if self.isReady():
            return
         await asyncio.sleep(0.1)

   ## rebalance ##
   async def onBalanceEvent(self, maker, taker):
      logging.debug("[HedgerFactory::onRebalanceEvent]")

   ## maker ##
   async def onMakerPositionEvent(self, maker, taker):
      logging.debug("[HedgerFactory::onMakerPositionEvent]")

   ## taker ##
   async def onTakerPositionEvent(self, maker, taker):
      logging.debug("[HedgerFactory::onTakerPositionEvent]")

   ## order book ##
   async def onTakerOrderBookEvent(self, maker, taker):
      logging.debug("[HedgerFactory::onTakerOrderBookEvent]")

   def getStatusStr(self):
      return "N/A"
