import Factories.Definitions as Definitions
import time

DEALER = 'dealer'
HEDGER = 'hedger'
MAKER = 'maker'
TAKER = 'taker'

class ReadyStatus(object):
   def __init__(self, factory):
      self.name = factory.name
      self.ready = factory.isReady()
      self.status = ""
      if not self.ready:
         self.status = factory.getStatusStr()

   def __str__(self):
      statusStr = "Ready"
      if not self.ready:
         statusStr = "Not Ready"
      result = f"  - {self.name}: {statusStr} -"

      if not self.ready:
         result += f"\n    !!! reason: {self.status} !!!"

      return result

class Factory(object):
   def __init__(self, config):
      self.lastPriceEvent = 0
      self.state = []

      self.balances = {
         MAKER : None,
         TAKER : None
      }

      self.positions = {
         MAKER : None,
         TAKER : None
      }
      self.config = config

   def getAsyncIOTask(self):
      return None

   async def report(self, event):
      pass

   async def onReadyEvent(self, dealer):
      self.state.clear()
      self.state.append(ReadyStatus(dealer))
      self.state.append(ReadyStatus(dealer.hedger))
      self.state.append(ReadyStatus(dealer.maker))
      self.state.append(ReadyStatus(dealer.taker))
      await self.report(Definitions.Ready)
      await self.onPositionEvent(dealer)

   async def onBalanceEvent(self, dealer):
      changes = False

      #maker balances
      makerBalance = dealer.maker.getBalance()
      if self.balances[MAKER] != makerBalance:
         self.balances[MAKER] = makerBalance
         changes = True

      #taker balances
      takerBalance = dealer.taker.getBalance()
      if self.balances[TAKER] != takerBalance:
         self.balances[TAKER] = takerBalance
         changes = True

      #only notify on __eq__ changes
      #the __eq__ operators are tailored to ignore certain
      #changes, such as pnl
      if changes:
         await self.report(Definitions.Balance)

   async def onPositionEvent(self, dealer):
      changes = False

      #maker
      makerPos = dealer.maker.getPositions()
      if self.positions[MAKER] != makerPos:
         self.positions[MAKER] = makerPos
         changes = True

      #taker
      takerPos = dealer.taker.getPositions()
      if self.positions[TAKER] != takerPos:
         self.positions[TAKER] = takerPos
         changes = True

      if changes:
         await self.report(Definitions.Position)

   async def onPriceEvent(self, dealer):
      if time.time() - self.lastPriceEvent < 30:
         return
      self.lastPriceEvent = time.time()

      self.balances[MAKER] = dealer.maker.getBalance()
      self.balances[TAKER] = dealer.taker.getBalance()
      await self.report(Definitions.PriceEvent)

   async def onRebalanceEvent(self, dealer):
      self.rebalance = dealer.hedger.getRebalanceStatus()
      await self.report(Definitions.Rebalance)