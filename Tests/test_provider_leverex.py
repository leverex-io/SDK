import unittest
from unittest.mock import patch

from .utils import TestTaker
from Hedger.SimpleHedger import SimpleHedger
from Factories.Dealer.Factory import DealerFactory

from Factories.Definitions import SessionInfo, \
   SessionOpenInfo, SessionCloseInfo

from Providers.Leverex import LeverexProvider
from Providers.leverex_core.api_connection import LeverexOrder, \
   ORDER_STATUS_FILLED, ORDER_TYPE_TRADE_POSITION, \
   ORDER_TYPE_NORMAL_ROLLOVER_POSITION

#import pdb; pdb.set_trace()

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
         'currency' : 'USDT',
         'balance' : self.balance
      }])
      self.balance_callback = None

   async def load_open_positions(self, target_product, callback):
      self.positions_callback = callback

   async def replyLoadPositions(self, orders):
      if self.positions_callback == None:
         raise Exception("positions where not requested")

      leverexOrders = []
      for order in orders:
         order['product_type'] = self.session_product
         leverexOrders.append(LeverexOrder(order))

      await self.positions_callback(leverexOrders)
      self.positions_callback = None

   async def subscribe_session_open(self, product):
      self.session_product = product

   async def subscribe_to_product(self, product):
      pass

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

   def push_market_data(self, price):
      self.listener.on_market_data({
         'live_cutoff' : str(price)
      })

########
class TestLeverexProvider(unittest.IsolatedAsyncioTestCase):
   config = {}
   config['leverex'] = {
      'api_endpoint' : 'the_endpoint',
      'login_endpoint' : 'login_endpoint',
      'key_file_path' : 'key/path',
      'email' : 'user_email',
      'product' : 'xbtusd_rf'
   }
   config['hedging_settings'] = {
      'price_ratio' : 0.01,
      'max_offer_volume' : 5,
      'offer_refresh_delay_ms' : 0
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

      #setup dealer
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
      assert dealer.isReady() == False

      #setup taker, we shouldn't generate offers until maker is ready
      await taker.updateBalance(1500)
      assert len(mockedConnection.offers) == 0

      await taker.populateOrderBook(10)
      assert len(mockedConnection.offers) == 0

      #Leverex authorized event (login successful)
      await maker.on_authorized()
      assert maker.isReady() == False
      assert dealer.isReady() == False
      assert maker._connected == True
      assert len(mockedConnection.offers) == 0

      #provider shouldn't be able to return exposure until it's ready
      assert maker.getExposure() == None

      #reply to load positions
      assert mockedConnection.positions_callback != None
      await mockedConnection.replyLoadPositions([])
      assert mockedConnection.positions_callback == None
      assert maker.isReady() == False
      assert dealer.isReady() == False
      assert maker._positionInitialized == True
      assert len(mockedConnection.offers) == 0

      #provider shouldn't be able to return exposure until it's ready
      assert maker.getExposure() == None

      #reply to load balances request
      assert mockedConnection.balance_callback != None
      assert len(maker.balances) == 0
      await mockedConnection.replyLoadBalances()
      assert mockedConnection.balance_callback == None
      assert maker.balances['USDT'] == 1000
      assert maker.isReady() == False
      assert dealer.isReady() == False
      assert maker._balanceInitialized == True
      assert len(mockedConnection.offers) == 0

      #provider shouldn't be able to return exposure until it's ready
      assert maker.getExposure() == None

      #reply to session sub
      assert mockedConnection.session_product != None
      await mockedConnection.notifySessionOpen(
         2, #session_id
         10000, #open price
         0 #open timestamp
      )
      assert maker.isReady() == True
      assert dealer.isReady() == True
      assert maker.getExposure() == 0

      assert len(mockedConnection.offers) == 1
      offers0 = mockedConnection.offers[0]
      assert len(offers0) == 1
      assert offers0[0].volume == 1
      assert offers0[0].bid == round(9981.25  * 0.99, 2)
      assert offers0[0].ask == round(10018.75 * 1.01, 2)

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

      #setup dealer
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
      assert dealer.isReady() == False

      #provider shouldn't be able to return exposure until it's ready
      assert maker.getExposure() == None

      await taker.populateOrderBook(10)
      assert len(mockedConnection.offers) == 0
      assert maker.isReady() == False
      assert dealer.isReady() == False

      #Leverex authorized event (login successful)
      await maker.on_authorized()
      assert maker.isReady() == False
      assert dealer.isReady() == False
      assert maker._connected == True
      assert len(mockedConnection.offers) == 0

      #provider shouldn't be able to return exposure until it's ready
      assert maker.getExposure() == None

      #reply to session sub
      assert mockedConnection.session_product != None
      await mockedConnection.notifySessionOpen(
         3, #session_id
         10000, #open price
         0 #open timestamp
      )
      assert len(mockedConnection.offers) == 0
      assert maker.isReady() == False
      assert dealer.isReady() == False

      #provider shouldn't be able to return exposure until it's ready
      assert maker.getExposure() == None

      #reply to load balances request
      assert mockedConnection.balance_callback != None
      assert len(maker.balances) == 0
      await mockedConnection.replyLoadBalances()
      assert mockedConnection.balance_callback == None
      assert maker.balances['USDT'] == 1000
      assert maker.isReady() == False
      assert dealer.isReady() == False
      assert maker._balanceInitialized == True
      assert len(mockedConnection.offers) == 0

      #provider shouldn't be able to return exposure until it's ready
      assert maker.getExposure() == None

      #reply to load positions
      assert mockedConnection.positions_callback != None
      await mockedConnection.replyLoadPositions([])
      assert maker.isReady() == True
      assert dealer.isReady() == True
      assert maker._positionInitialized == True
      assert mockedConnection.positions_callback == None
      assert maker.getExposure() == 0

      assert len(mockedConnection.offers) == 1
      offers0 = mockedConnection.offers[0]
      assert len(offers0) == 1
      assert offers0[0].volume == 1
      assert offers0[0].bid == round(9981.25  * 0.99, 2)
      assert offers0[0].ask == round(10018.75 * 1.01, 2)

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

      #setup dealer
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
      assert dealer.isReady() == False

      #provider shouldn't be able to return exposure until it's ready
      assert maker.getExposure() == None

      await taker.populateOrderBook(10)
      assert len(mockedConnection.offers) == 0

      #Leverex authorized event (login successful)
      await maker.on_authorized()
      assert maker.isReady() == False
      assert dealer.isReady() == False
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

      #provider shouldn't be able to return exposure until it's ready
      assert maker.getExposure() == None

      #reply to load positions
      assert mockedConnection.positions_callback != None
      await mockedConnection.replyLoadPositions([])
      assert mockedConnection.positions_callback == None
      assert maker.isReady() == False
      assert dealer.isReady() == False
      assert maker._positionInitialized == True
      assert len(mockedConnection.offers) == 0

      #provider shouldn't be able to return exposure until it's ready
      assert maker.getExposure() == None

      #reply to load balances request
      assert mockedConnection.balance_callback != None
      assert len(maker.balances) == 0
      await mockedConnection.replyLoadBalances()
      assert mockedConnection.balance_callback == None
      await dealer.waitOnReady()
      assert maker.isReady() == True
      assert dealer.isReady() == True
      assert maker._balanceInitialized == True
      assert maker.balances['USDT'] == 1000
      assert maker.getExposure() == 0

      assert len(mockedConnection.offers) == 1
      offers0 = mockedConnection.offers[0]
      assert len(offers0) == 1
      assert offers0[0].volume == 1
      assert offers0[0].bid == round(9981.25  * 0.99, 2)
      assert offers0[0].ask == round(10018.75 * 1.01, 2)

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

      #setup dealer
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
      assert dealer.isReady() == False

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
      assert maker.balances['USDT'] == 1000

      #reply to session sub
      assert mockedConnection.session_product != None
      await mockedConnection.notifySessionOpen(
         5, #session_id
         10000, #open price
         0 #open timestamp
      )
      assert maker.isReady() == True
      assert dealer.isReady() == True

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

      #check pnl
      mockedConnection.push_market_data(10200)
      posrep = maker.getPositions()
      assert len(posrep.positions) == 1
      pos1 = posrep.positions[1]
      assert pos1.trade_pnl == 100

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

      #check pnl
      posrep = maker.getPositions()
      assert len(posrep.positions) == 2
      pos1 = posrep.positions[1]
      assert pos1.trade_pnl == 100
      pos2 = posrep.positions[2]
      assert pos2.trade_pnl == -75

      #set price over cap, check pnl
      mockedConnection.push_market_data(12000)
      posrep = maker.getPositions()
      assert len(posrep.positions) == 2
      pos1 = posrep.positions[1]
      assert pos1.trade_pnl == 1000
      pos2 = posrep.positions[2]
      assert pos2.trade_pnl == -500

      #one last time
      mockedConnection.push_market_data(10000)
      posrep = maker.getPositions()
      assert len(posrep.positions) == 2
      pos1 = posrep.positions[1]
      assert pos1.trade_pnl == -100
      pos2 = posrep.positions[2]
      assert pos2.trade_pnl == 25

   #cover exposure sync at startup with existing maker orders
   @patch('Providers.Leverex.AsyncApiConnection')
   async def test_exposure_sync_startup(self, MockedLeverexConnObj):
      #setup mocked leverex connections
      mockedConnection = MockedLeverexConnectionClass(1000)
      MockedLeverexConnObj.return_value = mockedConnection

      #setup dealer
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
      assert dealer.isReady() == False

      #Leverex authorized event (login successful)
      await maker.on_authorized()
      assert maker.isReady() == False
      assert maker._connected == True

      #reply to load balances request
      assert mockedConnection.balance_callback != None
      assert len(maker.balances) == 0
      await mockedConnection.replyLoadBalances()
      assert mockedConnection.balance_callback == None
      assert maker.balances['USDT'] == 1000
      assert maker.isReady() == False
      assert dealer.isReady() == False

      #reply to session sub
      assert mockedConnection.session_product != None
      await mockedConnection.notifySessionOpen(
         10, #session_id
         10000, #open price
         0 #open timestamp
      )
      assert maker.isReady() == False
      assert dealer.isReady() == False

      orders = []

      #second order, comes first in list to cover order sorting
      #edge case around rollovers
      orders.append({
         'id' : 5,
         'timestamp' : 5,
         'quantity' : 0.5,
         'price' : 10100,
         'status' : ORDER_STATUS_FILLED,
         'reference_exposure' : 0,
         'session_id' : 10,
         'rollover_type' : ORDER_TYPE_TRADE_POSITION,
         'fee' : 7.5
      })

      #first order, rollover
      orders.append({
         'id' : 2,
         'timestamp' : 1,
         'quantity' : -1,
         'price' : 10000,
         'status' : ORDER_STATUS_FILLED,
         'reference_exposure' : -1,
         'session_id' : 10,
         'rollover_type' : ORDER_TYPE_NORMAL_ROLLOVER_POSITION,
         'fee' : 0
      })

      #third order
      orders.append({
         'id' : 6,
         'timestamp' : 15,
         'quantity' : -0.1,
         'price' : 10200,
         'status' : ORDER_STATUS_FILLED,
         'reference_exposure' : 0,
         'session_id' : 10,
         'rollover_type' : ORDER_TYPE_TRADE_POSITION,
         'fee' : 1.5
      })

      #check pnl
      mockedConnection.push_market_data(10200)
      posrep = maker.getPositions()
      assert len(posrep.positions) == 0

      #reply to load positions
      assert mockedConnection.positions_callback != None
      await mockedConnection.replyLoadPositions(orders)
      assert mockedConnection.positions_callback == None
      assert len(mockedConnection.offers) == 0
      assert maker.isReady() == True
      assert dealer.isReady() == True

      #check exposure
      assert maker.getExposure() == -0.6
      assert taker.getExposure() == 0.6

      #check pnl
      posrep = maker.getPositions()
      assert len(posrep.positions) == 3
      pos2 = posrep.positions[2]
      assert pos2.trade_pnl == -200
      pos5 = posrep.positions[5]
      assert pos5.trade_pnl == 50
      pos6 = posrep.positions[6]
      assert pos6.trade_pnl == 0

      #order for 0.2
      await mockedConnection.push_new_order({
         'id' : 12,
         'timestamp' : 20,
         'quantity' : 0.2,
         'price' : 10050,
         'status' : ORDER_STATUS_FILLED,
         'reference_exposure' : 0,
         'session_id' : 10,
         'rollover_type' : ORDER_TYPE_TRADE_POSITION,
         'fee' : 3
      })

      #check exposure
      assert maker.getExposure() == -0.4
      assert taker.getExposure() == 0.4

      #check pnl
      mockedConnection.push_market_data(9800)
      posrep = maker.getPositions()
      assert len(posrep.positions) == 4
      pos2 = posrep.positions[2]
      assert pos2.trade_pnl == 200
      pos5 = posrep.positions[5]
      assert pos5.trade_pnl == -150
      pos6 = posrep.positions[6]
      assert pos6.trade_pnl == 40
      pos12 = posrep.positions[12]
      assert pos12.trade_pnl == -50

   #cover new order handling and exposure signals
   @patch('Providers.Leverex.AsyncApiConnection')
   async def test_session_roll(self, MockedLeverexConnObj):
      #return mocked leverex connection object instead of an instance
      #of leverex_core.api_connection.AsyncApiConnection
      mockedConnection = MockedLeverexConnectionClass(1000)
      MockedLeverexConnObj.return_value = mockedConnection

      #setup dealer
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
      assert dealer.isReady() == False

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
      assert maker.balances['USDT'] == 1000

      #reply to session sub
      assert mockedConnection.session_product != None
      await mockedConnection.notifySessionOpen(
         5, #session_id
         10000, #open price
         0 #open timestamp
      )
      assert maker.isReady() == True
      assert dealer.isReady() == True

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

      #check pnl
      mockedConnection.push_market_data(10200)
      posrep = maker.getPositions()
      assert len(posrep.positions) == 1
      pos1 = posrep.positions[1]
      assert pos1.trade_pnl == 100

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

      #check pnl
      posrep = maker.getPositions()
      assert len(posrep.positions) == 2
      pos1 = posrep.positions[1]
      assert pos1.trade_pnl == 100
      pos2 = posrep.positions[2]
      assert pos2.trade_pnl == -75

      #set price over cap, check pnl
      mockedConnection.push_market_data(12000)
      posrep = maker.getPositions()
      assert len(posrep.positions) == 2
      pos1 = posrep.positions[1]
      assert pos1.trade_pnl == 1000
      pos2 = posrep.positions[2]
      assert pos2.trade_pnl == -500

      #one last time
      mockedConnection.push_market_data(10000)
      posrep = maker.getPositions()
      assert len(posrep.positions) == 2
      pos1 = posrep.positions[1]
      assert pos1.trade_pnl == -100
      pos2 = posrep.positions[2]
      assert pos2.trade_pnl == 25

      ## roll the session ##

      #push session end
      await mockedConnection.notifySessionClose(5)
      assert maker.isReady() == False
      assert taker.isReady() == True
      assert dealer.isReady() == False

      #TODO: previous orders should be FILLED first

      #push rolled over trade
      await mockedConnection.push_new_order({
         'id' : 3,
         'timestamp' : 1,
         'quantity' : 0.5,
         'price' : 10200,
         'status' : ORDER_STATUS_FILLED,
         'reference_exposure' : 0.5,
         'session_id' : 6,
         'rollover_type' : ORDER_TYPE_NORMAL_ROLLOVER_POSITION,
         'fee' : 0
      })

      #start new session
      await mockedConnection.notifySessionOpen(
         6, #id
         10200, #price
         1, #timestamp, ignored
      )

      assert maker.isReady() == True
      assert taker.isReady() == True
      assert dealer.isReady() == True

      #check pnl
      mockedConnection.push_market_data(10200)
      posrep = maker.getPositions()
      assert len(posrep.positions) == 1
      pos3 = posrep.positions[3]
      assert pos3.trade_pnl == 0

      #check pnl
      mockedConnection.push_market_data(10100)
      posrep = maker.getPositions()
      assert len(posrep.positions) == 1
      pos3 = posrep.positions[3]
      assert pos3.trade_pnl == -50