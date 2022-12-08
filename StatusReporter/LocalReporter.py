from datetime import datetime

from Factories.StatusReporter.Factory import Factory, MAKER, TAKER
from Factories.Definitions import Position, Balance, Ready

class LocalReporter(Factory):
   #### setup ####
   def __init__(self):
      super().__init__()

   def getAsyncIOTask(self):
      return None

   #### print statements ####
   def printReady(self):
      print (f"-- ready state update: --\n  {str(self.readyState)}")

   def printBalances(self):
      print (f"-- balance update:")

      #maker
      makerBalance = self.balances[MAKER]
      print (f"  * maker: {str(makerBalance)}")

      #taker
      takerBalance = self.balances[TAKER]
      print (f"  * taker:")
      for wallet in takerBalance:
         print (f"    +{wallet}:")
         for ccy in takerBalance[wallet]:
            print (f"      -{ccy}:{str(takerBalance[wallet][ccy])}")

   def printPositions(self):
      maker = self.positions[MAKER]
      taker = self.positions[TAKER]

      #provider timestamps are in ms
      timestamp = max(maker.timestamp, taker.timestamp) / 1000
      print (f"-- {datetime.fromtimestamp(timestamp)}: positions update --")

      print (str(maker))
      print (str(taker))

   #### report override ####
   async def report(self, notification):
      if notification == Ready:
         self.printReady()

      elif notification == Balance:
         self.printBalances()

      elif notification == Position:
         self.printPositions()