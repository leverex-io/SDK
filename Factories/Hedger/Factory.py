import logging
import asyncio

class HedgerFactory(object):
   def __init__(self, name):
      self._name = name
      self._ready = False
      self.onEventFunc = None

   ## setup ##
   def setup(self, onEventFunc):
      self.onEventFunc = onEventFunc

   ## ready ##
   async def onReadyEvent(self, maker, taker):
      logging.debug("[HedgerFactory::onReadyEvent]")

   @property
   def name(self):
      return self._name

   def isReady(self):
      return self._ready

   def setReady(self):
      if self.onEventFunc == None:
         raise Exception(f"Hedger {self._name} is missing event func")
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

   ## status ##
   def getStatusStr(self):
      if not self.isReady():
         return "waiting on exposure sync..."
      return "N/A"

   def getRebalanceStatus(self):
      logging.debug("[HedgerFactory::getRebalanceStatus]")
