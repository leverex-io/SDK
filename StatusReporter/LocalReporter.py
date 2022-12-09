import time
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
      print (f"-- STATUS: {datetime.fromtimestamp(time.time())} --")
      for state in self.state:
         print(state)

      print ("")

   def printBalances(self):
      print (f"-- BALANCE: {datetime.fromtimestamp(time.time())} --")

      print (str(self.balances[MAKER]))
      print (str(self.balances[TAKER]))

   def printPositions(self):
      print (f"-- POSITIONS: {datetime.fromtimestamp(time.time())} --")

      print (str(self.positions[MAKER]))
      print (str(self.positions[TAKER]))

   #### report override ####
   async def report(self, notification):
      if notification == Ready:
         self.printReady()

      elif notification == Balance:
         self.printBalances()

      elif notification == Position:
         self.printPositions()