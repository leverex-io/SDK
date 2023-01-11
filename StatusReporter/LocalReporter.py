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
      print (f"-- WALLETS: {datetime.fromtimestamp(time.time())} --")

      print (str(self.balances[MAKER]))
      print (str(self.balances[TAKER]))

   def printPositions(self):
      print (f"-- POSITIONS: {datetime.fromtimestamp(time.time())} --")

      print (str(self.positions[MAKER]))
      print (str(self.positions[TAKER]))

   def printPnl(self):
      print (f"-- PnL: {datetime.fromtimestamp(time.time())} --")

      print (self.positions[MAKER].getPnlReport())
      print (self.positions[TAKER].getPnlReport())
      print ("")

   def printRebalance(self):
      print (f"-- REBALANCE: {datetime.fromtimestamp(time.time())} --")

      print (str(self.rebalance))
      print ("")

   #### report override ####
   async def report(self, notification):
      if notification == Ready:
         self.printReady()

      elif notification == Balance:
         self.printBalances()

      elif notification == Position:
         self.printPositions()

      elif notification == PriceEvent:
         self.printPnl()

      elif notification == Rebalance:
         self.printRebalance()
