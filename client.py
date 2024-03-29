import logging
import asyncio
import json
import argparse
import time
import sys

#import pdb; pdb.set_trace()

from leverex_core.utils import LeverexOpenVolume, SessionOrders, \
   SessionInfo, SIDE_BUY, SIDE_SELL, ORDER_ACTION_CREATED, \
   round_down, Announcements
from leverex_core.base_client import LeverexBaseClient
from leverex_core.api_connection import PublicApiConnection

################################################################################
class LeverexClient(LeverexBaseClient):
   def __init__(self, config):
      super().__init__(config)
      self.setupConnection()
      self.takerFee = None
      self.public_connection = PublicApiConnection(
         config['leverex']['public_endpoint'])
      self.announcements = Announcements()

   async def subscribe(self):
      await super().subscribeToInitialData()

   async def public_subscribe(self):
      await self.public_connection.subscribe_session_open(self.product)
      await self.public_connection.subscribe_to_product(self.product)
      await self.public_connection.subscribe_dealer_offers(self.product)
      await self.public_connection.subscribe_to_announcements()

   ## asyncio loops
   async def parseCommand(self, command):
      if command == 'exit':
         loop = asyncio.get_event_loop()
         loop.stop()
         return False

      elif command == 'balance':
         self.printBalance()

      elif command == 'positions':
         self.printPositions()

      elif command == 'price':
         self.printPrice()

      elif command == 'address':
         async def printAddress(address):
            print (f" - deposit address: {address} - \n")
         await self.connection.load_deposit_address(printAddress)

      elif command == 'max':
         maxes = self.getMaxVolume()
         print (f" - max buy: {maxes['maxBid']}, sell: {maxes['maxAsk']}\n")

      elif command.startswith('buy'):
         value = command[3:].strip()
         if value == 'max':
            maxes = self.getMaxVolume()
            amount = maxes['maxBid']
            price = maxes['bid']
         else:
            value = float(value)
            offer = self.offers.getAsk(value)
            amount = min(value, offer.volume)
            price = offer.ask
         await self.placeOrder(amount, price)

      elif command.startswith('sell'):
         value = command[4:].strip()
         if value == 'max':
            maxes = self.getMaxVolume()
            amount = maxes['maxAsk']
            price = maxes['ask']
         else:
            value = float(value)
            offer = self.offers.getBid(value)
            amount = min(value, offer.volume)
            price = offer.bid
         await self.placeOrder(-amount, price)

      elif command == 'go flat':
         netExposure = -self.getExposure()
         if netExposure < 0:
            offer = self.offers.getBid(netExposure)
            if offer.volume < netExposure:
               print ("not enough bid size to go flat, nothing to do")
               pass
            price = offer.bid
         else:
            offer = self.offers.getAsk(netExposure)
            if offer.volume < netExposure:
               print ("not enough ask size to go flat, nothing to do")
               pass
            price = offer.ask

         await self.placeOrder(netExposure, price)

      elif command == 'session':
         self.printSession()

      elif command.startswith('announcements'):
         displayAll = False
         value = command[13:].strip()
         if value == 'all':
            displayAll = True
         elif value == 'max' or len(value) == 0:
            displayAll = False
         else:
            print ("invalid command")
            pass
         self.printAnnouncements(displayAll)

      elif command == 'help':
         helpStr = "- commands:\n"
         helpStr += "  . address: show deposit address\n"
         helpStr += "  . balance: show balances\n"
         helpStr += "  . positions: show positions, net exposure and pnl\n"
         helpStr += "  . price: show index price and offer streams\n"
         helpStr += "  . session: current session info\n"
         helpStr += "  . announcements [new/all]: display announcements.\n" \
            "      new: display only new/updated announcements\n" \
            "      all: display all announcements\n" \
            "      passing no arguments will default to new\n"
         helpStr += "  . max: show maximum buyable and sellable exposure\n"
         helpStr += "  . [buy/sell] [XXX]: place a long/short market order for XXX amount\n" \
            "      XXX is in XBT. Enter a max position with XXX set to [max], e.g.:\n" \
            "        buy 0.1: go long for 0.1 XBT\n" \
            "        sell 2: go short for 2 XBT\n" \
            "        buy max: go max long\n"
         helpStr += "  . go flat: place a market order that will result in your net exposure being 0\n"
         helpStr += "  . help: show this message\n"
         helpStr += "  . exit: shutdown the client\n"
         print (helpStr)

      else:
         print (f"unknown command: {command}")

      return True

   async def inputLoop(self, loop):
      keepRunning = True
      while keepRunning:
         print (">input a command>")
         command = await loop.run_in_executor(None, sys.stdin.readline)

         #strip the terminating \n
         if len(command) > 1 and command[-1] == '\n':
            command = command[0:-1].strip()
         keepRunning = await self.parseCommand(command)

   async def run(self):
      tasks = [asyncio.create_task(self.connection.run(self))]
      tasks.append(asyncio.create_task(self.public_connection.run(self)))

      done, pending = await asyncio.wait(
         tasks, return_when=asyncio.FIRST_COMPLETED)

   ## listeners
   def on_connected(self):
      print (f"connected to {self.config['leverex']['api_endpoint']}")

   def on_public_connected(self):
      print (f"connected to {self.config['leverex']['public_endpoint']}")

   async def on_authorized(self):
      await self.subscribe()
      await self.public_subscribe()

      loop = asyncio.get_event_loop()
      asyncio.ensure_future(self.inputLoop(loop))

   async def on_order_event(self, order, eventType):
      if eventType == ORDER_ACTION_CREATED:
         print (f"** new order: {str(order)} **")
      self.storeOrder(order, eventType)

   async def on_deposit_update(self, deposit_info):
      print (f"** detected deposit: {deposit_info.outputs}")

   async def on_dealer_offers(self, offers):
      self.offers = offers

   async def on_announcement(self, announcements):
      self.announcements.processUpdate(announcements)
      print ("new announcements!")

   ## max calcs
   def getMaxVolume(self):
      lov = LeverexOpenVolume(self)

      openVol = round_down(lov.openBalance / lov.session.getSessionIM(), 8)
      openVolAsk = openVolBid = openVol

      while (True):
         #look for stream with matching open volume
         #NOTE: we bid into the dealer's ask and vice versa
         ask = self.offers.getAsk(openVolBid)
         bid = self.offers.getBid(openVolAsk)

         bidPrice = 0
         if bid.isValid():
            bidPrice = bid.bid

         askPrice = 0
         if ask.isValid():
            askPrice = ask.ask

         #get releasble exposure
         openVolBid, openVolAsk = lov.getReleasableExposure(bidPrice, askPrice)
         matchedBid = None
         matchedAsk = None

         #loop again until releasable exposure fits in offer volume
         #or this is the biggest offer for this side
         bidIsReady = True
         if bid.isValid() and openVolAsk > bid.volume:
            if bid.isLast:
               openVolAsk = min(openVolAsk, bid.volume)
            else:
               bidIsReady = False

         askIsReady = True
         if ask.isValid() and openVolBid > ask.volume:
            if ask.isLast:
               openVolBid = min(openVolBid, ask.volume)
            else:
               askIsReady = False

         if bidIsReady and askIsReady:
            matchedBid = bid
            matchedAsk = ask
            break

      feeRate = lov.session.getSessionIM() / (lov.session.getSessionIM() + lov.session.getTakerFee())
      if matchedBid and openVolAsk < matchedBid.volume:
         #only withhold cost of fees from our ask if it's smaller than the bid offer's volume
         openVolAsk *= feeRate

      if matchedAsk and openVolBid < matchedAsk.volume:
         #only withhold cost of fees from our bid if it's smaller than the ask offer's volume
         openVolBid *= feeRate

      return {
         'ask' : bid.bid,
         'maxAsk' : round_down(openVolAsk, 8),
         'bid' : ask.ask,
         'maxBid' : round_down(openVolBid, 8)
      }

   async def placeOrder(self, amount, price):
      side = SIDE_BUY if amount > 0 else SIDE_SELL
      await self.connection.place_order(
         abs(round_down(amount, 8)),
         side,
         self.product, price
      )

   ## printers
   def printBalance(self):
      balanceStr = "Balances:\n"

      if self.balances == None:
         balanceStr += (" - N/A")
      else:
         total = 0
         for account in self.balances:
            total += self.balances[account]
            balanceStr += f" - account: {account}, amount: {self.balances[account]}\n"
         balanceStr += f" - total: {total}\n"

      print (balanceStr)

   def printPrice(self):
      prices = f"- index price: {self.indexPrice}\n"
      if self.currentSession:
         prices += f"- session open price: {self.currentSession.getOpenPrice()}\n"
      prices += "- bids:\n"
      for offer in self.offers.bids:
         prices += f"   . {str(offer)}\n"

      prices += "- asks:\n"
      for offer in self.offers.asks:
         prices += f"   . {str(offer)}\n"

      print (prices)

   def printPositions(self):
      try:
         exposure = self.getExposure()
         pnl = self.getTotalPnl()
         orders = self.getSessionOrders()

         positionStr = ""
         if not orders:
            positionStr = "   . N/A\n"
         else:
            for orderId in orders:
               positionStr += f"   . {str(orders[orderId])}\n"
         positionStr += f" - net exposure: {exposure}, pnl: {pnl}\n"
      except:
         positionStr = "   . N/A"

      print (f" - Positions:\n{positionStr}")

   def printSession(self):
      if self.currentSession == None:
         print (f" - Session: N/A\n")

      prefix = "    "
      print (f" - Session:\n{self.currentSession.prettyPrint(prefix)}\n")

   def printAnnouncements(self, displayAll):
      print (self.announcements.toString(displayAll))

################################################################################
if __name__ == '__main__':
   LOG_FORMAT = (
      "%(asctime)s,%(msecs)d %(levelname)-8s [%(filename)s:%(lineno)d] %(message)s"
   )
   logging.basicConfig(level=logging.INFO, format=LOG_FORMAT)

   parser = argparse.ArgumentParser(description='Leverex Client')

   parser.add_argument('--config', type=str, help='Config file to use')

   args = parser.parse_args()

   config = {}
   with open(args.config) as json_config_file:
      config = json.load(json_config_file)

   try:
      client = LeverexClient(config)
      asyncio.run(client.run())
   except Exception as e:
      logging.error(f"!! Main loop broke with error: {str(e)} !!")
