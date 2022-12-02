import Factories.Definitions as Definitions

class Factory(object):
   def __init__(self):
      self.readyState = {
         'dealer' : False,
         'hedger' : False,
         'maker'  : False,
         'taker'  : False
      }

   async def report(self, event):
      pass

   async def onReadyEvent(self, dealer):
      self.readyState['dealer'] = dealer.isReady()
      self.readyState['hedger'] = dealer.hedger.isReady()
      self.readyState['maker'] = dealer.maker.isReady()
      self.readyState['taker'] = dealer.taker.isReady()
      await self.report(Definitions.Ready)