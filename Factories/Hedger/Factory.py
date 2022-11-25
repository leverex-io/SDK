import logging
import asyncio

class HedgerFactory(object):
   def __init__(self):
      pass

   ## ready ##
   async def onReadyEvent(self, maker, taker):
      logging.debug("[HedgerFactory::onReadyEvent]")

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
