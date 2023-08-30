import logging
import asyncio
import json
import argparse
import time
import sys

from Providers.leverex_core.api_connection import AsyncApiConnection, \
   ORDER_ACTION_UPDATED, ORDER_ACTION_CREATED
from Providers.leverex_core.product_mapping import get_product_info
from Providers.Leverex import LeverexOpenVolume, SessionOrders
from Factories.Definitions import SessionInfo, getBalancesFromJson, \
   SIDE_BUY, SIDE_SELL

################################################################################
class LeverexClient(object):
   def __init__(self, config):
      self.config = config
      self.connection = None
      self.send = None

      self.product = self.config['leverex']['product']
      productInfo = get_product_info(self.product)
      self.ccy = productInfo.cash_ccy
      self.margin_ccy = productInfo.margin_ccy

      self.setupConnection()
      self.orderData = {}

      self.indexPrice = 0
      self.balances = None
      self.currentSession = None
      self.takerFee = None

   ## setup
   def setupConnection(self):
      leverexConfig = self.config['leverex']
      keyPath = None
      if 'key_file_path' in leverexConfig:
         keyPath = leverexConfig['key_file_path']

      aeid_endpoint = None
      if 'aeid' in self.config and 'endpoint' in self.config['aeid']:
         aeid_endpoint = self.config['aeid']['endpoint']

      self.connection = AsyncApiConnection(
         api_endpoint=leverexConfig['api_endpoint'],
         login_endpoint=leverexConfig['login_endpoint'],
         key_file_path=keyPath,
         dump_communication=False,
         aeid_endpoint=aeid_endpoint)

   async def subscribe(self):
      await self.connection.subscribe_to_product(self.product)
      await self.connection.subscribe_session_open(self.product)
      await self.connection.subscribe_to_balance_updates(self.product)
      await self.connection.subscribe_dealer_offers(self.product)
      await self.connection.product_fee(self.product, self.setTakerFee)
      await self.connection.load_open_positions(
         target_product=self.product,
         callback=self.on_positions_loaded)

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
            print (f" - deposit address: {address} - ")
         await self.connection.load_deposit_address(printAddress)

      elif command == 'max':
         maxes = self.getMaxVolume()
         print (f" - max buy: {maxes['maxBid']}, sell: {maxes['maxAsk']}")

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
         netExposure = -self.getNetExposure()
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

      elif command == 'help':
         helpStr = "- commands:\n"
         helpStr += "  . address: show deposit address\n"
         helpStr += "  . balance: show balances\n"
         helpStr += "  . positions: show positions, net exposure and pnl\n"
         helpStr += "  . price: show index price and offer streams\n"
         helpStr += "  . max: show maximum buyable and sellable exposure\n"
         helpStr += "  . buy/sell XXX: place a long/short market order for XXX amount\n" \
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
      run = True
      while (run):
         print (">input a command>")
         command = await loop.run_in_executor(None, sys.stdin.readline)

         #strip the terminating \n
         if len(command) > 1 and command[-1] == '\n':
            command = command[0:-1].strip()
         run = await self.parseCommand(command)

   async def run(self):
      tasks = [asyncio.create_task(self.connection.run(self))]

      done, pending = await asyncio.wait(
         tasks, return_when=asyncio.FIRST_COMPLETED)

   ## listeners
   def on_connected(self):
      print (f"connected to {self.config['leverex']['api_endpoint']}")

   async def on_authorized(self):
      await self.subscribe()

      loop = asyncio.get_event_loop()
      asyncio.ensure_future(self.inputLoop(loop))

   async def on_positions_loaded(self, orders):
      for order in orders:
         self.storeOrder(order, ORDER_ACTION_UPDATED)

   async def on_order_event(self, order, eventType):
      if eventType == ORDER_ACTION_CREATED:
         print (f"** new order: {str(order)} **")
      self.storeOrder(order, eventType)

   async def on_session_open(self, sessionInfo):
      self.setSession(SessionInfo(sessionInfo))

   async def on_session_closed(self, sessionInfo):
      self.setSession(SessionInfo(sessionInfo))

   async def on_market_data(self, marketData):
      self.setPrice(float(marketData['live_cutoff']))

   async def on_deposit_update(self, deposit_info):
      print (f"** detected deposit: {deposit_info.ouputs}")

   async def on_dealer_offers(self, offers):
      self.offers = offers

   ## price and session events ##
   def setPrice(self, price):
      self.indexPrice = price

   def setSession(self, session):
      self.currentSession = session
      sessionId = session.getSessionId()
      if sessionId not in self.orderData:
         self.orderData[sessionId] = SessionOrders(sessionId)
      self.orderData[sessionId].setSessionObj(session)

   async def setTakerFee(self, feeReply):
      if feeReply['success'] == True:
         self.takerFee = float(feeReply['fee'])

   ## max calcs
   def getMaxVolume(self):
      lov = LeverexOpenVolume(self)

      openVol = lov.openBalance / lov.session.getSessionIM()
      openVolAsk = openVolBid = openVol

      while (True):
         #look for stream with matching open volume
         #NOTE: we bid into the dealer's ask and vice versa
         ask = self.offers.getAsk(openVolAsk)
         bid = self.offers.getBid(openVolBid)

         #get releasble exposure
         maxBuy, maxSell = lov.getReleasableExposure(bid.bid, ask.ask)
         openVolAsk = openVol + maxSell
         openVolBid = openVol + maxBuy

         #loop again until releasable exposure fits in offer volume
         #or this is the biggest offer for this side
         if openVolAsk > bid.volume and not bid.isLast:
            continue
         elif openVolBid > ask.volume and not ask.isLast:
            continue
         else:
            break

      feeRate = lov.session.getSessionIM() / (lov.session.getSessionIM() + self.takerFee)
      openVolAsk *= feeRate
      openVolBid *= feeRate

      return {
         'ask' : bid.bid,
         'maxAsk' : round(openVolAsk, 8),
         'bid' : ask.ask,
         'maxBid' : round(openVolBid, 8)
      }

   def getSessionOrders(self):
      currentSessionId = None
      if self.currentSession:
         currentSessionId = self.currentSession.getSessionId()
      if not currentSessionId:
         raise Exception()

      sessionOrders = self.orderData[currentSessionId]
      if not sessionOrders:
         raise Exception()
      return sessionOrders.orders

   def getNetExposure(self):
      orders = self.getSessionOrders()
      netExposure = 0
      for orderId in orders:
         order = orders[orderId]
         netExposure += \
            order.quantity if not order.is_sell() \
            else -order.quantity
      return round(netExposure, 8)

   def getTotalPnl(self):
      orders = self.getSessionOrders()
      totalPnl = 0
      for orderId in orders:
         order = orders[orderId]
         order.setIndexPrice(self.indexPrice)
         order.computePnL()
         totalPnl += order.trade_pnl
      return round(totalPnl, 6)

   ## orders ##
   def storeOrder(self, order, eventType):
      sessionId = order.session_id
      if sessionId not in self.orderData:
         #create SessionOrders object
         self.orderData[sessionId] = SessionOrders(sessionId)

         #set session object if we have one
         if self.currentSession != None and \
            self.currentSession.getSessionId() == sessionId:
            self.orderData[sessionId].setSessionObj(self.currentSession)

      self.orderData[sessionId].setOrder(order, eventType)

   async def placeOrder(self, amount, price):
      side = SIDE_BUY if amount > 0 else SIDE_SELL
      await self.connection.place_order(
         abs(round(amount, 8)),
         side,
         self.product, price
      )

   ## balance events ##
   async def on_balance_update(self, balances):
      self.balances = getBalancesFromJson(balances)

   ## printers
   def printBalance(self):
      balanceStr = "Balances:\n"

      if self.balances == None:
         balanceStr += (" - N/A")
      else:
         for account in self.balances:
            balanceStr += f" - account: {account}, amount: {self.balances[account]}\n"
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
         exposure = self.getNetExposure()
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

################################################################################
if __name__ == '__main__':
   LOG_FORMAT = (
      "%(asctime)s,%(msecs)d %(levelname)-8s [%(filename)s:%(lineno)d] %(message)s"
   )
   logging.basicConfig(level=logging.INFO, format=LOG_FORMAT)

   parser = argparse.ArgumentParser(description='Leverex Bitfinix Dealer') 

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