import Factories.Definitions as Definitions

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
   def __init__(self):
      self.state = []

      self.balances = {
         MAKER : None,
         TAKER : None
      }

      self.positions = {
         MAKER : None,
         TAKER : None
      }

   def getAsyncIOTask(self):
      return None

   async def report(self, event):
      pass

   async def onReadyEvent(self, dealer):
      self.state = []
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

      #only notify on balance changes
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
