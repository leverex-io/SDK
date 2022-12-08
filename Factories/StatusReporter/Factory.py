import Factories.Definitions as Definitions

DEALER = 'dealer'
HEDGER = 'hedger'
MAKER = 'maker'
TAKER = 'taker'

class Factory(object):
   def __init__(self):
      self.readyState = {
         DEALER : False,
         HEDGER : False,
         MAKER  : False,
         TAKER  : False
      }

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
      self.readyState[DEALER] = dealer.isReady()
      self.readyState[HEDGER] = dealer.hedger.isReady()
      self.readyState[MAKER]  = dealer.maker.isReady()
      self.readyState[TAKER]  = dealer.taker.isReady()
      await self.report(Definitions.Ready)

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
