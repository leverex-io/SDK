'''
hedger tests:

- price offer triggers:
    . session start
    . session end/halted (stop quoting)
    . free exposure changes (both maker and taker):
        * balance changes
        * margin changes (this should cover maker entering a new trade)
    . order book changes (taker only)
'''
#import pdb; pdb.set_trace()

import unittest
from unittest.mock import Mock, create_autospec, patch
import asyncio

from Factories.Provider.Factory import Factory
from Factories.Definitions import AggregationOrderBook, \
   Order, SessionInfo, SessionOpenInfo, SessionCloseInfo
from Hedger.SimpleHedger import SimpleHedger
from Factories.Dealer.Factory import DealerFactory
from Providers.Leverex import LeverexProvider
from Providers.leverex_core.api_connection import LeverexOrder, \
   ORDER_STATUS_FILLED, ORDER_TYPE_TRADE_POSITION

price = 10000

################################################################################
##
#### Order book tests
##
################################################################################
class TestOrderBook(unittest.TestCase):
   def test_price_agg(self):
      orderBook = AggregationOrderBook()

      ## asks, price should be above the index price ##
      orderBook.process_update([10010, 1, -1])
      orderBook.process_update([10050, 1, -2])
      orderBook.process_update([10100, 1, -5])

      #0.5
      result = orderBook.get_aggregated_ask_price(0.5)
      self.assertEqual(result.price, 10010)
      self.assertEqual(result.volume, 1)

      #1
      result = orderBook.get_aggregated_ask_price(1)
      self.assertEqual(result.price, 10036.67)
      self.assertEqual(result.volume, 3)

      #2
      result = orderBook.get_aggregated_ask_price(2)
      self.assertEqual(result.price, 10036.67)
      self.assertEqual(result.volume, 3)

      #5
      result = orderBook.get_aggregated_ask_price(5)
      self.assertEqual(result.price, 10076.25)
      self.assertEqual(result.volume, 8)


      #7
      result = orderBook.get_aggregated_ask_price(7)
      self.assertEqual(result.price, 10076.25)
      self.assertEqual(result.volume, 8)

      #10
      result = orderBook.get_aggregated_ask_price(10)
      self.assertEqual(result.price, 10076.25)
      self.assertEqual(result.volume, 8)

      ## bids, price should be below the index price ##
      orderBook.process_update([9990, 1, 1])
      orderBook.process_update([9950, 1, 2])
      orderBook.process_update([9900, 1, 5])

      #0.5
      result = orderBook.get_aggregated_bid_price(0.5)
      self.assertEqual(result.price, 9990)
      self.assertEqual(result.volume, 1)

      #1
      result = orderBook.get_aggregated_bid_price(1)
      self.assertEqual(result.price, 9963.33)
      self.assertEqual(result.volume, 3)

      #2
      result = orderBook.get_aggregated_bid_price(2)
      self.assertEqual(result.price, 9963.33)
      self.assertEqual(result.volume, 3)

      #5
      result = orderBook.get_aggregated_bid_price(5)
      self.assertEqual(result.price, 9923.75)
      self.assertEqual(result.volume, 8)


      #7
      result = orderBook.get_aggregated_bid_price(7)
      self.assertEqual(result.price, 9923.75)
      self.assertEqual(result.volume, 8)

      #10
      result = orderBook.get_aggregated_bid_price(10)
      self.assertEqual(result.price, 9923.75)
      self.assertEqual(result.volume, 8)

################################################################################
##
#### Test providers
##
################################################################################
class TestProvider(Factory):
   def __init__(self, leverageRatio, startBalance=0):
      super().__init__()

      self.startBalance = startBalance
      self.balance = 0
      self.leverageRatio = leverageRatio

   async def getAsyncIOTask(self):
      return asyncio.create_task(self.bootstrap())

   async def bootstrap(self):
      await self.initBalance(self.startBalance)

   async def setReady(self, isReady):
      super().setConnected(isReady)
      await self.onReady()

   async def initBalance(self, balance):
      super().setInitBalance()
      await self.updateBalance(balance)

   async def updateBalance(self, balance):
      self.balance = balance
      await super().onBalanceUpdate()

   async def initPositions(self):
      super().setInitPosition()
      await super().onPositionUpdate()

   def getOpenVolume(self):
      if self.isReady() == False:
         return None

      vol = ( self.balance * 100 ) / ( self.leverageRatio * price )
      exposure = self.getExposure()
      bid = vol - exposure
      ask = vol + exposure
      return { 'ask' : ask, 'bid' : bid }

########
class TestMaker(TestProvider):
   def __init__(self, startBalance=0, startPositions=[]):
      super().__init__(10, startBalance)

      self.startPositions = startPositions
      self.offers = []
      self.orders = []

   async def bootstrap(self):
      await super().bootstrap()
      await self.setReady(True)
      await self.initPositions(self.startPositions)

   async def initPositions(self, startPositions):
      self.orders.extend(startPositions)
      await super().initPositions()

   async def submitOffers(self, offers):
      self.offers.append(offers)

   async def newOrder(self, order):
      self.orders.append(order)
      await super().onPositionUpdate()

   def getExposure(self):
      if not super().isReady():
         return None

      exposure = 0
      for order in self.orders:
         orderQ = order.quantity
         if order.is_sell:
            orderQ *= -1
         exposure += orderQ

      return round(exposure, 8)

########
class TestTaker(TestProvider):
   def __init__(self, startBalance=0, startExposure=0):
      super().__init__(15, startBalance)

      self.startExposure = startExposure
      self.order_book = AggregationOrderBook()
      self.exposure = 0

   async def bootstrap(self):
      await super().bootstrap()
      await self.setReady(True)
      await self.initExposure(self.startExposure)

   async def initExposure(self, startExposure):
      self.exposure = startExposure
      await super().initPositions()

   async def populateOrderBook(self, volume):
      self.order_book.reset()

      vol = volume / 2
      for i in range(0, 5):
         spread = 20*vol
         self.order_book.process_update([price + spread, 1, -vol]) #ask
         self.order_book.process_update([price - spread, 1,  vol]) #bid
         vol = vol / 2

      await super().onOrderBookUpdate()

   def getExposure(self):
      if not super().isReady():
         return None
      return self.exposure

   async def updateExposure(self, exposure):
      self.exposure += exposure
      await super().onPositionUpdate()

################################################################################
##
#### Hedger Tests
##
################################################################################
class TestHedger(unittest.IsolatedAsyncioTestCase):
   config = {}
   config['hedging_settings'] = {}
   config['hedging_settings']['price_ratio'] = 0.01
   config['hedging_settings']['max_offer_volume'] = 5

   async def test_offers_signals(self):
      #test hedger making/pulling offers
      maker = TestMaker()
      taker = TestTaker()
      hedger = SimpleHedger(self.config)
      dealer = DealerFactory(maker, taker, hedger)
      await dealer.run()

      #set taker order book, we shouldn't generate offers until maker is ready
      await taker.updateBalance(15000)
      self.assertEqual(len(maker.offers), 0)

      await taker.populateOrderBook(10)
      self.assertEqual(len(maker.offers), 0)

      #setup maker
      await maker.updateBalance(10000)
      self.assertEqual(len(maker.offers), 1)

      #check the offers
      offers0 = maker.offers[0]
      self.assertEqual(len(offers0), 1)

      #shutdown maker, offers should be pulled
      await maker.setReady(False)
      self.assertEqual(len(maker.offers), 2)

      #check the offers
      offers1 = maker.offers[1]
      self.assertEqual(len(offers1), 0)

      #restart maker, we should get offers once again
      await maker.setReady(True)
      self.assertEqual(len(maker.offers), 3)

      #check the offers
      offers2 = maker.offers[2]
      self.assertEqual(len(offers2), 1)

      #shutdown taker, offers should be pulled
      await taker.setReady(False)
      self.assertEqual(len(maker.offers), 4)

      #check the offers
      offers3 = maker.offers[3]
      self.assertEqual(len(offers3), 0)

      #shutdown maker, no offers should be added
      await maker.setReady(False)
      self.assertEqual(len(maker.offers), 4)

   async def test_offers_volume(self):
      taker = TestTaker(startBalance=1500)
      maker = TestMaker(startBalance=1000)

      hedger = SimpleHedger(self.config)
      dealer = DealerFactory(maker, taker, hedger)
      await dealer.run()

      #we should have offers yet
      self.assertEqual(len(maker.offers), 0)

      #quote and check price & volumes of offers
      await taker.populateOrderBook(10)
      self.assertEqual(len(maker.offers), 1)

      offers0 = maker.offers[0]
      self.assertEqual(len(offers0), 1)
      self.assertEqual(offers0[0].volume, 1)
      self.assertEqual(offers0[0].bid, round(9981.25  * 0.99, 2))
      self.assertEqual(offers0[0].ask, round(10018.75 * 1.01, 2))

      #balance event
      await maker.updateBalance(500)
      self.assertEqual(len(maker.offers), 2)

      offers1 = maker.offers[1]
      self.assertEqual(len(offers1), 1)
      self.assertEqual(offers1[0].volume, 0.5)
      self.assertEqual(offers1[0].bid, round(9989.58  * 0.99, 2))
      self.assertEqual(offers1[0].ask, round(10010.42 * 1.01, 2))

      #order book event
      await taker.populateOrderBook(6)
      self.assertEqual(len(maker.offers), 3)

      offers2 = maker.offers[2]
      self.assertEqual(len(offers2), 1)
      self.assertEqual(offers2[0].volume, 0.5)
      self.assertEqual(offers2[0].bid, round(9993.75  * 0.99, 2))
      self.assertEqual(offers2[0].ask, round(10006.25 * 1.01, 2))

   async def test_offers_order(self):
      #maker orders should affect maker and taker exposure accordingly
      #effect of order should be reflected on margins, and on offers
      taker = TestTaker(startBalance=1500)
      maker = TestMaker(startBalance=500)

      hedger = SimpleHedger(self.config)
      dealer = DealerFactory(maker, taker, hedger)
      await dealer.run()
      self.assertEqual(len(maker.offers), 0)

      #order book event
      await taker.populateOrderBook(6)
      self.assertEqual(len(maker.offers), 1)

      offers0 = maker.offers[0]
      self.assertEqual(len(offers0), 1)
      self.assertEqual(offers0[0].volume, 0.5)
      self.assertEqual(offers0[0].bid, round(9993.75  * 0.99, 2))
      self.assertEqual(offers0[0].ask, round(10006.25 * 1.01, 2))

      #new order event
      newOrder = Order(id=1, timestamp=0, quantity=0.1, price=10100)
      await maker.newOrder(newOrder)
      self.assertEqual(len(maker.offers), 2)

      #check exposure
      self.assertEqual(maker.getExposure(), 0.1)
      self.assertEqual(taker.getExposure(), -0.1)

      #check volumes
      makerVolume = maker.getOpenVolume()
      self.assertEqual(makerVolume['ask'], 0.6)
      self.assertEqual(makerVolume['bid'], 0.4)

      takerVolume = taker.getOpenVolume()
      self.assertEqual(takerVolume['ask'], 0.9)
      self.assertEqual(takerVolume['bid'], 1.1)

      #check offers
      offers1 = maker.offers[1]
      self.assertEqual(len(offers1), 2)

      self.assertEqual(offers1[0].volume, 0.6)
      self.assertEqual(offers1[0].bid, None)
      self.assertEqual(offers1[0].ask, round(10011.25 * 1.01, 2))

      self.assertEqual(offers1[1].volume, 0.4)
      self.assertEqual(offers1[1].bid, round(9993.75  * 0.99, 2))
      self.assertEqual(offers1[1].ask, None)

   async def test_exposure_sync(self):
      #this test sets various exposure on the maker and the taker,
      #then starts the dealer and checks the exposures are in check

      #setup taker and maker
      taker = TestTaker(startBalance=1500)

      startOrders = []
      startOrders.append(Order(id=1, timestamp=0, quantity=0.1, price=10100))
      startOrders.append(Order(id=2, timestamp=0, quantity=0.2, price=10150))
      maker = TestMaker(startBalance=1000, startPositions=startOrders)

      #check they have no balance nor exposure pre dealer start
      assert maker.getExposure() == None
      assert taker.getExposure() == None
      self.assertEqual(maker.balance, 0)
      self.assertEqual(taker.balance, 0)

      hedger = SimpleHedger(self.config)
      dealer = DealerFactory(maker, taker, hedger)
      await dealer.run()

      #check taker exposure is the opposite of the maker's
      self.assertEqual(maker.balance, 1000)
      self.assertEqual(taker.balance, 1500)
      self.assertEqual(maker.getExposure(), 0.3)
      self.assertEqual(taker.getExposure(), -0.3)

      #add another order
      newOrder = Order(id=3, timestamp=0, quantity=-0.1, price=9900)
      await maker.newOrder(newOrder)

      self.assertEqual(maker.balance, 1000)
      self.assertEqual(taker.balance, 1500)
      self.assertEqual(maker.getExposure(), 0.2)
      self.assertEqual(taker.getExposure(), -0.2)

################################################################################
##
#### Leverex Provider Tests
##
################################################################################
class MockedLeverexConnectionClass(object):
   def __init__(self, balance=0):
      self.offers = []
      self.listener = None
      self.balance = balance

      self.session_product = None
      self.balance_callback = None
      self.positions_callback = None

   async def run(self, listener):
      self.listener = listener
      pass

   async def submit_offers(self, target_product, offers, callback):
      self.offers.append(offers)

   def loadBalances(self, callback):
      self.balance_callback = callback

   async def replyLoadBalances(self):
      if self.balance_callback == None:
         raise Exception("balances were not requested")

      await self.balance_callback([{
         'currency' : 'usdt',
         'balance' : self.balance
      }])
      self.balance_callback = None

   async def load_open_positions(self, target_product, callback):
      self.positions_callback = callback

   async def replyLoadPositions(self, orders):
      if self.positions_callback == None:
         raise Exception("positions where not requested")
      await self.positions_callback(orders)
      self.positions_callback = None

   async def subscribe_session_open(self, product):
      self.session_product = product

   async def notifySessionOpen(self, session_id, open_price, timestamp):
      await self.listener.on_session_open(SessionOpenInfo({
         'product_type' : self.session_product,
         'cut_off_at' : timestamp,
         'last_cut_off_price' : open_price,
         'session_id' : session_id,
         'previous_session_id' : session_id - 1
      }))

   async def notifySessionClose(self, session_id):
      await self.listener.on_session_closed(SessionCloseInfo({
         'product_type' : self.session_product,
         'session_id' : session_id
      }))

   async def push_new_order(self, order):
      order['product_type'] = self.session_product
      await self.listener.on_order_created(LeverexOrder(order))

########
class TestLeverexProvider(unittest.IsolatedAsyncioTestCase):
   config = {}
   config['leverex'] = {
      'api_endpoint' : 'the_endpoint',
      'login_endpoint' : 'login_endpoint',
      'key_file_path' : 'key/path',
      'email' : 'user_email'
   }
   config['hedging_settings'] = {
      'leverex_product' : 'usdt',
      'price_ratio' : 0.01,
      'max_offer_volume' : 5
   }

   '''
   NOTE: bootstrap tests cover the Leverex provider handling of
         various events around dealer start and stop. They do
         not overlap with the hedger bootstrap test, the check
         the hedger handling of the same events.
   '''

   #session notification last
   @patch('Providers.Leverex.AsyncApiConnection')
   async def test_bootstrap_1(self, MockedLeverexConnObj):
      #return mocked leverex connection object instead of an instance
      #of leverex_core.api_connection.AsyncApiConnection
      mockedConnection = MockedLeverexConnectionClass(1000)
      MockedLeverexConnObj.return_value = mockedConnection

      #test hedger making/pulling offers
      maker = LeverexProvider(self.config)
      taker = TestTaker()
      hedger = SimpleHedger(self.config)
      dealer = DealerFactory(maker, taker, hedger)
      await dealer.run()

      #sanity check on mocked connection
      assert maker.connection is mockedConnection
      assert maker.connection.listener is maker
      assert maker.isReady() == False
      assert taker.isReady() == True

      #setup taker, we shouldn't generate offers until maker is ready
      await taker.updateBalance(1500)
      assert len(mockedConnection.offers) == 0

      await taker.populateOrderBook(10)
      assert len(mockedConnection.offers) == 0

      #Leverex authorized event (login successful)
      await maker.on_authorized()
      assert maker.isReady() == False
      assert maker._connected == True
      assert len(mockedConnection.offers) == 0

      #reply to load positions
      assert mockedConnection.positions_callback != None
      await mockedConnection.replyLoadPositions([])
      assert mockedConnection.positions_callback == None
      assert maker.isReady() == False
      assert maker._positionInitialized == True
      assert len(mockedConnection.offers) == 0

      #reply to load balances request
      assert mockedConnection.balance_callback != None
      assert len(maker.balances) == 0
      await mockedConnection.replyLoadBalances()
      assert mockedConnection.balance_callback == None
      assert maker.balances['usdt'] == 1000
      assert maker.isReady() == False
      assert maker._balanceInitialized == True
      assert len(mockedConnection.offers) == 0

      #reply to session sub
      assert mockedConnection.session_product != None
      await mockedConnection.notifySessionOpen(
         2, #session_id
         10000, #open price
         0 #open timestamp
      )
      assert maker.isReady() == True

      assert len(mockedConnection.offers) == 1
      offers0 = mockedConnection.offers[0]
      self.assertEqual(len(offers0), 1)
      self.assertEqual(offers0[0].volume, 1)
      self.assertEqual(offers0[0].bid, round(9981.25  * 0.99, 2))
      self.assertEqual(offers0[0].ask, round(10018.75 * 1.01, 2))

      #close session, should pull offers
      await mockedConnection.notifySessionClose(2) #session_id
      assert len(mockedConnection.offers) == 2
      assert len(mockedConnection.offers[1]) == 0

   #load position reply last
   @patch('Providers.Leverex.AsyncApiConnection')
   async def test_bootstrap_2(self, MockedLeverexConnObj):
      #return mocked leverex connection object instead of an instance
      #of leverex_core.api_connection.AsyncApiConnection
      mockedConnection = MockedLeverexConnectionClass(1000)
      MockedLeverexConnObj.return_value = mockedConnection

      #test hedger making/pulling offers
      maker = LeverexProvider(self.config)
      taker = TestTaker(startBalance=1500)
      hedger = SimpleHedger(self.config)
      dealer = DealerFactory(maker, taker, hedger)
      await dealer.run()

      #sanity check on mocked connection
      assert maker.connection is mockedConnection
      assert maker.connection.listener is maker
      assert maker.isReady() == False
      assert taker.isReady() == True

      await taker.populateOrderBook(10)
      assert len(mockedConnection.offers) == 0
      assert maker.isReady() == False

      #Leverex authorized event (login successful)
      await maker.on_authorized()
      assert maker.isReady() == False
      assert maker._connected == True
      assert len(mockedConnection.offers) == 0

      #reply to session sub
      assert mockedConnection.session_product != None
      await mockedConnection.notifySessionOpen(
         3, #session_id
         10000, #open price
         0 #open timestamp
      )
      assert len(mockedConnection.offers) == 0
      assert maker.isReady() == False

      #reply to load balances request
      assert mockedConnection.balance_callback != None
      assert len(maker.balances) == 0
      await mockedConnection.replyLoadBalances()
      assert mockedConnection.balance_callback == None
      assert maker.balances['usdt'] == 1000
      assert maker.isReady() == False
      assert maker._balanceInitialized == True
      assert len(mockedConnection.offers) == 0

      #reply to load positions
      assert mockedConnection.positions_callback != None
      await mockedConnection.replyLoadPositions([])
      assert maker.isReady() == True
      assert maker._positionInitialized == True
      assert mockedConnection.positions_callback == None

      assert len(mockedConnection.offers) == 1
      offers0 = mockedConnection.offers[0]
      self.assertEqual(len(offers0), 1)
      self.assertEqual(offers0[0].volume, 1)
      self.assertEqual(offers0[0].bid, round(9981.25  * 0.99, 2))
      self.assertEqual(offers0[0].ask, round(10018.75 * 1.01, 2))

      #close session, should pull offers
      await mockedConnection.notifySessionClose(3) #session_id
      assert len(mockedConnection.offers) == 2
      assert len(mockedConnection.offers[1]) == 0

   #load balances reply last
   @patch('Providers.Leverex.AsyncApiConnection')
   async def test_bootstrap_3(self, MockedLeverexConnObj):
      #return mocked leverex connection object instead of an instance
      #of leverex_core.api_connection.AsyncApiConnection
      mockedConnection = MockedLeverexConnectionClass(1000)
      MockedLeverexConnObj.return_value = mockedConnection

      #test hedger making/pulling offers
      maker = LeverexProvider(self.config)
      taker = TestTaker(startBalance=1500)
      hedger = SimpleHedger(self.config)
      dealer = DealerFactory(maker, taker, hedger)
      await dealer.run()

      #sanity check on mocked connection
      assert maker.connection is mockedConnection
      assert maker.connection.listener is maker
      assert maker.isReady() == False
      assert taker.isReady() == True

      await taker.populateOrderBook(10)
      assert len(mockedConnection.offers) == 0

      #Leverex authorized event (login successful)
      await maker.on_authorized()
      assert maker.isReady() == False
      assert maker._connected == True
      assert len(mockedConnection.offers) == 0

      #reply to session sub
      assert mockedConnection.session_product != None
      await mockedConnection.notifySessionOpen(
         4, #session_id
         10000, #open price
         0 #open timestamp
      )
      assert maker.isReady() == False
      assert len(mockedConnection.offers) == 0

      #reply to load positions
      assert mockedConnection.positions_callback != None
      await mockedConnection.replyLoadPositions([])
      assert mockedConnection.positions_callback == None
      assert maker.isReady() == False
      assert maker._positionInitialized == True
      assert len(mockedConnection.offers) == 0

      #reply to load balances request
      assert mockedConnection.balance_callback != None
      assert len(maker.balances) == 0
      await mockedConnection.replyLoadBalances()
      assert mockedConnection.balance_callback == None
      assert maker.isReady() == True
      assert maker._balanceInitialized == True
      assert maker.balances['usdt'] == 1000

      assert len(mockedConnection.offers) == 1
      offers0 = mockedConnection.offers[0]
      self.assertEqual(len(offers0), 1)
      self.assertEqual(offers0[0].volume, 1)
      self.assertEqual(offers0[0].bid, round(9981.25  * 0.99, 2))
      self.assertEqual(offers0[0].ask, round(10018.75 * 1.01, 2))

      #close session, should pull offers
      await mockedConnection.notifySessionClose(4) #session_id
      assert len(mockedConnection.offers) == 2
      assert len(mockedConnection.offers[1]) == 0

   #cover new order handling and exposure signals
   @patch('Providers.Leverex.AsyncApiConnection')
   async def test_exposure_sync(self, MockedLeverexConnObj):
      #return mocked leverex connection object instead of an instance
      #of leverex_core.api_connection.AsyncApiConnection
      mockedConnection = MockedLeverexConnectionClass(1000)
      MockedLeverexConnObj.return_value = mockedConnection

      maker = LeverexProvider(self.config)
      taker = TestTaker(startBalance=1500)
      hedger = SimpleHedger(self.config)
      dealer = DealerFactory(maker, taker, hedger)
      await dealer.run()

      #sanity check on mocked connection
      assert maker.connection is mockedConnection
      assert maker.connection.listener is maker
      assert maker.isReady() == False
      assert taker.isReady() == True

      #Leverex authorized event (login successful)
      await maker.on_authorized()
      assert maker.isReady() == False
      assert maker._connected == True

      #reply to load positions
      assert mockedConnection.positions_callback != None
      await mockedConnection.replyLoadPositions([])
      assert mockedConnection.positions_callback == None
      assert len(mockedConnection.offers) == 0

      #reply to load balances request
      assert mockedConnection.balance_callback != None
      assert len(maker.balances) == 0
      await mockedConnection.replyLoadBalances()
      assert mockedConnection.balance_callback == None
      assert maker.balances['usdt'] == 1000

      #reply to session sub
      assert mockedConnection.session_product != None
      await mockedConnection.notifySessionOpen(
         5, #session_id
         10000, #open price
         0 #open timestamp
      )
      assert maker.isReady() == True

      #push new order for 1btc
      assert maker.getExposure() == 0
      assert taker.getExposure() == 0
      await mockedConnection.push_new_order({
         'id' : 1,
         'timestamp' : 1,
         'quantity' : 1,
         'price' : 10100,
         'status' : ORDER_STATUS_FILLED,
         'reference_exposure' : 0,
         'session_id' : 5,
         'rollover_type' : ORDER_TYPE_TRADE_POSITION,
         'fee' : 15
      })
      assert maker.getExposure() == 1
      assert taker.getExposure() == -1

      #order for -0.5
      await mockedConnection.push_new_order({
         'id' : 2,
         'timestamp' : 1,
         'quantity' : -0.5,
         'price' : 10050,
         'status' : ORDER_STATUS_FILLED,
         'reference_exposure' : 0,
         'session_id' : 5,
         'rollover_type' : ORDER_TYPE_TRADE_POSITION,
         'fee' : 7.5
      })
      assert maker.getExposure() == 0.5
      assert taker.getExposure() == -0.5

################################################################################
if __name__ == '__main__':
   unittest.main()