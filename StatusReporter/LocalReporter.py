import time
from datetime import datetime

from Factories.StatusReporter.Factory import Factory, MAKER, TAKER
from Factories.Definitions import Position, Balance, Ready, \
   PriceEvent, Rebalance

class LocalReporter(Factory):
   #### setup ####
   def __init__(self, config):
      super().__init__(config)

   def getAsyncIOTask(self):
      return None

   #### print statements ####
   def printReady(self):
      print (f"-- STATUS: {datetime.fromtimestamp(time.time())} --")
      for state in self.state:
         print(state)
      print ("")

   def printBalances(self):
      print (f"++ WALLETS: {datetime.fromtimestamp(time.time())} ++")
      makerBalance = self.balances[MAKER]
      takerBalance = self.balances[TAKER]
      if makerBalance is None:
         makerBalance = "maker: N/A"
      else:
         makerBalance = str(makerBalance)
      if takerBalance is None:
         takerBalance = "taker: N/A"
      else:
         takerBalance = str(takerBalance)
      final = makerBalance + " +\n" + takerBalance
      print (final)

   def printPositions(self):
      print (f"** POSITIONS: {datetime.fromtimestamp(time.time())} **")
      final = str(self.positions[MAKER]) + " *\n" + str(self.positions[TAKER])
      print (final)

   def printPriceEvent(self):
      print (f"$$ PRICE UPDATE: {datetime.fromtimestamp(time.time())} $$")

      print (" $  - PNL:")
      print (self.positions[MAKER].getPnlReport())
      print (self.positions[TAKER].getPnlReport())
      print (" $\n $  - OFFERS:")
      print (self.offers)

   def printRebalance(self):
      print (f"-- REBALANCE: {datetime.fromtimestamp(time.time())} --")

      print (str(self.rebalance))
      print ("")

   #### report override ####
   async def report(self, notification):
      try:
         if notification == Ready:
            self.printReady()

         elif notification == Balance:
            self.printBalances()

         elif notification == Position:
            self.printPositions()

         elif notification == PriceEvent:
            self.printPriceEvent()

         elif notification == Rebalance:
            self.printRebalance()
      except Exception as e:
         print ("failed to print report with exception: {e}")
