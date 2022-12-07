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
      print (f"-- positions update:")

      #maker
      makerPositions = self.positions[MAKER]

      #grab index price from first order
      leverexPrice = ""
      if len(makerPositions) != 0:
         firstPos = next(iter(makerPositions))
         leverexPrice = " (index price: {})".format(
            makerPositions[firstPos].indexPrice)

      print (f"  * maker{leverexPrice}:")
      for pos in makerPositions:
         print (f"    +{str(makerPositions[pos])}")

      #taker
      takerPositions = self.positions[TAKER]
      print (f"  * taker:")
      for product in takerPositions:
         print (f"    +{str(takerPositions[product])}")

   #### report override ####
   async def report(self, notification):
      if notification == Ready:
         self.printReady()

      elif notification == Balance:
         self.printBalances()

      elif notification == Position:
         self.printPositions()