import logging
import asyncio
import Factories.Definitions as Definitions

class Factory(object):
   ## setup ##
   def __init__(self):
      self.dealerCallback = None
      self._connected = False
      self._balanceInitialized = False
      self._positionInitialized = False

   def setup(self, callback):
      if callback == None:
         raise Definitions.ProviderException("missing hedging callback")
      self.dealerCallback = callback

   async def getAsyncIOTask(self):
      pass

   def setInitBalance(self):
      if self._balanceInitialized:
         raise Definitions.ProviderException("init failure")
      self._balanceInitialized = True

   def setInitPosition(self):
      if self._positionInitialized:
         raise Definitions.ProviderException("init failure")
      self._positionInitialized = True

   def setConnected(self, value):
      self._connected = value

   def isReady(self):
      return self._connected and \
         self._balanceInitialized and \
         self._positionInitialized

   ## notifications ##
   async def onReady(self):
      await self.dealerCallback(self, Definitions.Ready)

   def onNewPrice(self, price):
      pass

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
      logging.debug("[updateExposure]")

   def withdraw(self, withdrawInfo):
      logging.debug("[withdraw]")

   async def submitOffers(self, offers):
      logging.debug("[submitOffers]")

   ## getters ##
   def getExposure(self):
      logging.debug("[getPostion]")

   def getOrderBook(self):
      logging.debug("[getOrderBook]")

   def getOpenVolume(self):
      logging.debug("[getOpenVolume]")