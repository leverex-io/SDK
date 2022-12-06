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
      self.balances[MAKER] = dealer.maker.getBalance()
      self.balances[TAKER] = dealer.taker.getBalance()
      await self.report(Definitions.Balance)

   async def onPositionEvent(self, dealer):
      self.positions[MAKER] = dealer.maker.getPositions()
      self.positions[TAKER] = dealer.taker.getPositions()
      await self.report(Definitions.Position)
