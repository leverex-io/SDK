import unittest
from unittest.mock import patch
from typing import Callable
import time

from .utils import TestTaker, price
from Hedger.SimpleHedger import SimpleHedger
from Factories.Dealer.Factory import DealerFactory

from leverex_core.utils import SessionInfo, \
   SessionOpenInfo, SessionCloseInfo, \
   SIDE_BUY, SIDE_SELL, LeverexOrder, \
   ORDER_STATUS_FILLED, ORDER_STATUS_PENDING, \
   ORDER_TYPE_TRADE_POSITION, ORDER_TYPE_NORMAL_ROLLOVER_POSITION, \
   ORDER_ACTION_CREATED, ORDER_ACTION_UPDATED, WithdrawInfo

from Providers.Leverex import LeverexProvider

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
      self.positions_callback = None
      self.withdrawRequest = []
      self.cancelWithdraw = []

      self.orders = {}
      self.indexPrice = None
      self.pendingWtdr = {}

   async def run(self, listener):
      self.listener = listener
      pass

   async def submit_offers(self, target_product, offers, callback):
      self.offers.append(offers)

   async def subscribe_to_balance_updates(self, target_product):
      if self.session_product != target_product:
         raise Exception("balance sub product mismatch")

   async def pushBalanceUpdate(self):
      freeMargin = 0
      for orderId in self.orders:
         order = self.orders[orderId]
         if order.is_filled():
            #filled orders do not affect exposure
            continue

         margin = order.getMargin()
         if margin == None:
            continue

         #apply margin to relevant side
         if order.is_sell():
            freeMargin -= margin
         else:
            freeMargin += margin

      freeAskMargin = 0
      freeBidMargin = 0
      if freeMargin > 0:
         freeAskMargin = abs(freeMargin) * 2
      else:
         freeBidMargin = abs(freeMargin) * 2

      margin = abs(self.getMarginedCash())
      openBalance = self.balance - margin

      balanceSection = [{
         'currency' : 'USDT',
         'balance' : str(openBalance),
      }]
      if margin != 0:
         balanceSection.append({
         'currency' : 'USDP',
         'balance' : str(margin)
      })

      result = {
         'balances' : balanceSection
      }
      await self.listener.on_balance_update(result)

   async def load_open_positions(self, target_product, callback):
      self.positions_callback = callback

   async def replyLoadPositions(self, orders):
      if self.positions_callback == None:
         raise Exception("positions where not requested")

      leverexOrders = []
      for order in orders:
         order['product_type'] = self.session_product
         side = SIDE_BUY
         if order['quantity'] < 0:
            side = SIDE_SELL
         order['side'] = side
         order['is_taker'] = False

         levOrder = LeverexOrder(order)
         leverexOrders.append(levOrder)
         self.orders[levOrder.id] = levOrder

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
         'previous_session_id' : session_id - 1,
         'healthy' : True
      }))

   async def notifySessionClose(self, session_id):
      await self.listener.on_session_closed(SessionCloseInfo({
         'product_type' : self.session_product,
         'session_id' : session_id,
         'healthy' : True
      }))

   async def notifySessionBroken(self, session_id):
      await self.listener.on_session_closed(SessionCloseInfo({
         'product_type' : self.session_product,
         'session_id' : session_id,
         'healthy' : False
      }))

   async def push_new_order(self, order):
      order['product_type'] = self.session_product
      side = SIDE_BUY
      if order['quantity'] < 0:
         side = SIDE_SELL
      order['side'] = side
      order['is_taker'] = False
      levOrder = LeverexOrder(order)

      self.orders[levOrder.id] = levOrder
      await self.listener.on_order_event(levOrder, ORDER_ACTION_CREATED)
      await self.pushBalanceUpdate()

   async def close_order(self, order):
      order['product_type'] = self.session_product
      side = SIDE_BUY
      if order['quantity'] < 0:
         side = SIDE_SELL
      order['side'] = side
      order['is_taker'] = False

      levOrder = LeverexOrder(order)
      del self.orders[levOrder.id]
      await self.listener.on_order_event(levOrder, ORDER_ACTION_UPDATED)

   async def push_market_data(self, price):
      await self.listener.on_market_data({
         'live_cutoff' : str(price)
      })

   async def load_deposit_address(self, callback):
      await callback("leverex_address")

   async def load_whitelisted_addresses(self, callback):
      await callback(["efgh", "ijkl"])

   async def load_withdrawals_history(self, callback):
      await callback([])

   async def withdraw_liquid(self, *, address, currency, amount, callback: Callable = None):
      self.withdrawRequest.append({
         'address' : address,
         'ccy' : currency,
         'amount' : amount,
         'callback' : callback
      })

   async def cancel_withdraw(self, *, id, callback: Callable = None):
      self.cancelWithdraw.append({
         'id': id,
         'callback': callback
      })

   async def confirmWithdrawalRequest(self):
      for withdrawal in self.withdrawRequest:

         self.balance -= withdrawal['amount']
         await self.pushBalanceUpdate()

         wtd = WithdrawInfo({
            'id' : 0,
            'recv_address' : withdrawal['address'],
            'currency' : withdrawal['ccy'],
            'amount' : withdrawal['amount'],
            'timestamp' : time.time(),
            'status' : 1
         })
         await withdrawal['callback'](wtd)

      self.withdrawRequest = []
      self.pendingWtdr[wtd.id] = wtd

   async def sendWithdrawalInfo(self, wInfo):
      self.pendingWtdr[wInfo.id] = wInfo
      await self.listener.on_withdraw_update(wInfo)

   async def completeWithdrawalRequest(self):
      for wId in self.pendingWtdr:
         wtd = self.pendingWtdr[wId]
         wtd._status = 4
         await self.sendWithdrawalInfo(wtd)
      self.pendingWtdr.clear()

   async def completeCancelRequest(self):
      for cancellation in self.cancelWithdraw:
         if cancellation['id'] not in self.pendingWtdr:
            continue

         wtdr = self.pendingWtdr[cancellation['id']]
         wtdr._status = 5
         del self.pendingWtdr[cancellation['id']]
         await cancellation['callback'](wtdr)

         self.balance += float(wtdr.amount)
         await self.pushBalanceUpdate()

   def getMarginedCash(self):
      #this is a simplified version of leverex margin calculation,
      #do not use this if you need accurate values
      margin = 0
      for orderId in self.orders:
         order = self.orders[orderId]
         val = order.quantity * 0.1 * price
         if self.indexPrice != None:
            pnl = (self.indexPrice - order.price) * order.quantity
            if order.is_sell():
               pnl *= -1
            if pnl < 0:
               val += pnl

         if order.is_sell():
            margin -= val
         else:
            margin += val
      return margin

   async def setIndexPrice(self, index_price):
      self.indexPrice = index_price
      await self.listener.on_market_data({
         'live_cutoff': index_price
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
   config['hedger'] = {
      'price_ratio' : 0.01,
      'max_offer_volume' : 5,
      'offer_refresh_delay_ms' : 0,
      'min_size' : 0.00006,
      'quote_ratio' : 0.2
   }
   config['rebalance'] = {
      'enable' : True,
      'threshold_pct' : 0.1,
      'min_amount' : 10
   }

   '''
   NOTE: bootstrap tests cover the Leverex provider handling of
         various events around dealer start and stop. They do
         not overlap with the hedger bootstrap test, the check
         the hedger handling of the same events.
   '''

   #session notification last
   @patch('leverex_core.base_client.AsyncApiConnection')
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
      assert len(maker.balances) == 0
      await mockedConnection.pushBalanceUpdate()
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
      assert offers0[0].volume == 0.8
      assert offers0[0].bid == round(9989.58  * 0.99, 2)
      assert offers0[0].ask == round(10010.42 * 1.01, 2)

      #close session, should pull offers
      await mockedConnection.notifySessionClose(2) #session_id
      assert len(mockedConnection.offers) == 2
      assert len(mockedConnection.offers[1]) == 0

   #load position reply last
   @patch('leverex_core.base_client.AsyncApiConnection')
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
      assert len(maker.balances) == 0
      await mockedConnection.pushBalanceUpdate()
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
      assert offers0[0].volume == 0.8
      assert offers0[0].bid == round(9989.58  * 0.99, 2)
      assert offers0[0].ask == round(10010.42 * 1.01, 2)

      #close session, should pull offers
      await mockedConnection.notifySessionClose(3) #session_id
      assert len(mockedConnection.offers) == 2
      assert len(mockedConnection.offers[1]) == 0

   #load balances reply last
   @patch('leverex_core.base_client.AsyncApiConnection')
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
      assert len(maker.balances) == 0
      await mockedConnection.pushBalanceUpdate()
      await dealer.waitOnReady()
      assert maker.isReady() == True
      assert dealer.isReady() == True
      assert maker._balanceInitialized == True
      assert maker.balances['USDT'] == 1000
      assert maker.getExposure() == 0

      assert len(mockedConnection.offers) == 1
      offers0 = mockedConnection.offers[0]
      assert len(offers0) == 1
      assert offers0[0].volume == 0.8
      assert offers0[0].bid == round(9989.58  * 0.99, 2)
      assert offers0[0].ask == round(10010.42 * 1.01, 2)

      #close session, should pull offers
      await mockedConnection.notifySessionClose(4) #session_id
      assert len(mockedConnection.offers) == 2
      assert len(mockedConnection.offers[1]) == 0

   #cover new order handling and exposure signals
   @patch('leverex_core.base_client.AsyncApiConnection')
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

      posrep = maker.getPositions()
      assert posrep.getOrderCount() == 0

      #reply to load positions
      assert mockedConnection.positions_callback != None
      await mockedConnection.replyLoadPositions([])
      assert mockedConnection.positions_callback == None
      assert len(mockedConnection.offers) == 0

      #reply to load balances request
      assert len(maker.balances) == 0
      await mockedConnection.pushBalanceUpdate()
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
      await mockedConnection.push_market_data(10000)
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
      await mockedConnection.push_market_data(10200)
      posrep = maker.getPositions()
      assert posrep.getOrderCount() == 1
      pos1 = posrep.orderData.orders[1]
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
      assert posrep.getOrderCount() == 2
      pos1 = posrep.orderData.orders[1]
      assert pos1.trade_pnl == 100
      pos2 = posrep.orderData.orders[2]
      assert pos2.trade_pnl == -75

      #set price over cap, check pnl
      await mockedConnection.push_market_data(12000)
      posrep = maker.getPositions()
      assert posrep.getOrderCount() == 2
      pos1 = posrep.orderData.orders[1]
      assert pos1.trade_pnl == 1000
      pos2 = posrep.orderData.orders[2]
      assert pos2.trade_pnl == -500

      #one last time
      await mockedConnection.push_market_data(10000)
      posrep = maker.getPositions()
      assert posrep.getOrderCount() == 2
      pos1 = posrep.orderData.orders[1]
      assert pos1.trade_pnl == -100
      pos2 = posrep.orderData.orders[2]
      assert pos2.trade_pnl == 25

   #cover exposure sync at startup with existing maker orders
   @patch('leverex_core.base_client.AsyncApiConnection')
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
      assert len(maker.balances) == 0
      await mockedConnection.pushBalanceUpdate()
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
         'status' : ORDER_STATUS_PENDING,
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
         'status' : ORDER_STATUS_PENDING,
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
         'status' : ORDER_STATUS_PENDING,
         'reference_exposure' : 0,
         'session_id' : 10,
         'rollover_type' : ORDER_TYPE_TRADE_POSITION,
         'fee' : 1.5
      })

      #check pnl
      await mockedConnection.push_market_data(10200)
      posrep = maker.getPositions()
      assert posrep.getOrderCount() == 0

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
      assert posrep.getOrderCount() == 3
      pos2 = posrep.orderData.orders[2]
      assert pos2.trade_pnl == -200
      pos5 = posrep.orderData.orders[5]
      assert pos5.trade_pnl == 50
      pos6 = posrep.orderData.orders[6]
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
      await mockedConnection.push_market_data(9800)
      posrep = maker.getPositions()
      assert posrep.getOrderCount() == 4
      pos2 = posrep.orderData.orders[2]
      assert pos2.trade_pnl == 200
      pos5 = posrep.orderData.orders[5]
      assert pos5.trade_pnl == -150
      pos6 = posrep.orderData.orders[6]
      assert pos6.trade_pnl == 40
      pos12 = posrep.orderData.orders[12]
      assert pos12.trade_pnl == -50

   #cover session end and roll overs
   @patch('leverex_core.base_client.AsyncApiConnection')
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
      assert len(maker.balances) == 0
      await mockedConnection.pushBalanceUpdate()
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

      orders = [{ #1 btc
            'id' : 1,
            'timestamp' : 1,
            'quantity' : 1,
            'price' : 10100,
            'status' : ORDER_STATUS_FILLED,
            'reference_exposure' : 0,
            'session_id' : 5,
            'rollover_type' : ORDER_TYPE_TRADE_POSITION,
            'fee' : 15 
         }, { #-0.5
            'id' : 2,
            'timestamp' : 1,
            'quantity' : -0.5,
            'price' : 10050,
            'status' : ORDER_STATUS_FILLED,
            'reference_exposure' : 0,
            'session_id' : 5,
            'rollover_type' : ORDER_TYPE_TRADE_POSITION,
            'fee' : 7.5
         }
      ]

      #push new order for 1btc
      await mockedConnection.push_market_data(10000)
      assert maker.getExposure() == 0
      assert taker.getExposure() == 0
      await mockedConnection.push_new_order(orders[0])
      assert maker.getExposure() == 1
      assert taker.getExposure() == -1

      #check pnl
      await mockedConnection.push_market_data(10200)
      posrep = maker.getPositions()
      assert posrep.getOrderCount() == 1
      pos1 = posrep.orderData.orders[1]
      assert pos1.trade_pnl == 100

      #order for -0.5
      await mockedConnection.push_new_order(orders[1])
      assert maker.getExposure() == 0.5
      assert taker.getExposure() == -0.5

      #check pnl
      posrep = maker.getPositions()
      assert posrep.getOrderCount() == 2
      pos1 = posrep.orderData.orders[1]
      assert pos1.trade_pnl == 100
      pos2 = posrep.orderData.orders[2]
      assert pos2.trade_pnl == -75

      #set price over cap, check pnl
      await mockedConnection.push_market_data(12000)
      posrep = maker.getPositions()
      assert posrep.getOrderCount() == 2
      pos1 = posrep.orderData.orders[1]
      assert pos1.trade_pnl == 1000
      pos2 = posrep.orderData.orders[2]
      assert pos2.trade_pnl == -500

      #one last time
      await mockedConnection.push_market_data(10000)
      posrep = maker.getPositions()
      assert posrep.getOrderCount() == 2
      pos1 = posrep.orderData.orders[1]
      assert pos1.trade_pnl == -100
      pos2 = posrep.orderData.orders[2]
      assert pos2.trade_pnl == 25

      ## roll the session ##

      #push session end
      await mockedConnection.notifySessionClose(5)
      assert maker.isReady() == False
      assert taker.isReady() == True
      assert dealer.isReady() == False
      assert maker.getExposure() == None
      assert taker.getExposure() == -0.5

      await mockedConnection.close_order(orders[0])
      await mockedConnection.close_order(orders[1])
      assert maker.getExposure() == None
      assert taker.getExposure() == -0.5

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
      await mockedConnection.push_market_data(10200)
      posrep = maker.getPositions()
      assert posrep.getOrderCount() == 1
      pos3 = posrep.orderData.orders[3]
      assert pos3.trade_pnl == 0

      #check pnl
      await mockedConnection.push_market_data(10100)
      posrep = maker.getPositions()
      assert posrep.getOrderCount() == 1
      pos3 = posrep.orderData.orders[3]
      assert pos3.trade_pnl == -50

   #break session, taker exposure should go to 0
   @patch('leverex_core.base_client.AsyncApiConnection')
   async def test_unhealthy_session(self, MockedLeverexConnObj):
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
      assert len(maker.balances) == 0
      await mockedConnection.pushBalanceUpdate()
      assert maker.balances['USDT'] == 1000

      #reply to session sub
      assert mockedConnection.session_product != None
      await mockedConnection.notifySessionOpen(
         5, #session_id
         10000, #open price
         0 #open timestamp
      )
      assert maker.isReady() == True
      assert maker.isBroken() == False
      assert dealer.isReady() == True

      await mockedConnection.push_market_data(10000)

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
      await mockedConnection.push_market_data(10200)
      posrep = maker.getPositions()
      assert posrep.getOrderCount() == 1
      pos1 = posrep.orderData.orders[1]
      assert pos1.trade_pnl == 100

      #break the session
      await mockedConnection.notifySessionBroken(5)
      assert maker.isReady() == False
      assert maker.isBroken() == True
      assert dealer.isReady() == False

      assert maker.getExposure() == None
      assert taker.getExposure() == 0

      #fix the session
      await mockedConnection.notifySessionOpen(
         6, #session_id
         10000, #open price
         1 #open timestamp
      )

      assert maker.isReady() == True
      assert maker.isBroken() == False
      assert dealer.isReady() == True

      assert maker.getExposure() == 0
      assert taker.getExposure() == 0

   #counterparty provider collateral should adjust to expected value
   #even though positions are opened at a different leverage
   @patch('leverex_core.base_client.AsyncApiConnection')
   async def test_adjust_collateral(self, MockedLeverexConnObj):
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
      assert len(maker.balances) == 0
      await mockedConnection.pushBalanceUpdate()
      assert maker.balances['USDT'] == 1000

      #reply to session sub
      assert mockedConnection.session_product != None
      await mockedConnection.notifySessionOpen(
         5, #session_id
         10000, #open price
         0 #open timestamp
      )
      assert maker.isReady() == True
      assert maker.isBroken() == False
      assert dealer.isReady() == True
      assert taker.targetCollateral == None

      #push new order for 1btc
      assert maker.getExposure() == 0
      assert taker.getExposure() == 0
      await mockedConnection.push_market_data(10000)
      order = {
         'id' : 1,
         'timestamp' : 1,
         'quantity' : 1,
         'price' : 10100,
         'status' : ORDER_STATUS_FILLED,
         'reference_exposure' : 0,
         'session_id' : 5,
         'rollover_type' : ORDER_TYPE_TRADE_POSITION,
         'fee' : 15
      }
      await mockedConnection.push_new_order(order)
      assert maker.getExposure() == 1
      assert taker.getExposure() == -1
      assert taker.targetCollateral == 1500

      #close the session
      await mockedConnection.notifySessionClose(5)
      assert maker.isReady() == False
      assert maker.isBroken() == False
      assert dealer.isReady() == False

      assert maker.getExposure() == None
      assert taker.getExposure() == -1
      assert taker.targetCollateral == None

      await mockedConnection.close_order(order)
      assert maker.getExposure() == None
      assert taker.getExposure() == -1
      assert taker.targetCollateral == None

      #push rolled over trade
      await mockedConnection.push_new_order({
         'id' : 3,
         'timestamp' : 1,
         'quantity' : 1,
         'price' : 10200,
         'status' : ORDER_STATUS_FILLED,
         'reference_exposure' : 1,
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
      assert maker.isBroken() == False
      assert dealer.isReady() == True

      assert maker.getExposure() == 1
      assert taker.getExposure() == -1
      assert taker.targetCollateral == 1530

      await mockedConnection.push_new_order({
         'id' : 5,
         'timestamp' : 1,
         'quantity' : -0.5,
         'price' : 10300,
         'status' : ORDER_STATUS_FILLED,
         'reference_exposure' : 0,
         'session_id' : 6,
         'rollover_type' : ORDER_TYPE_TRADE_POSITION,
         'fee' : 7.5
      })
      assert maker.getExposure() == 0.5
      assert taker.getExposure() == -0.5
      assert taker.targetCollateral == 765

   #cover open volume asymetry
   @patch('leverex_core.base_client.AsyncApiConnection')
   async def test_open_volume(self, MockedLeverexConnObj):
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
      assert len(maker.balances) == 0
      await mockedConnection.pushBalanceUpdate()
      assert maker.balances['USDT'] == 1000

      #reply to session sub
      assert mockedConnection.session_product != None
      await mockedConnection.notifySessionOpen(
         5, #session_id
         10000, #open price
         0 #open timestamp
      )

      #check providers and hedger are ready
      assert maker.isReady() == True
      assert maker.isBroken() == False
      assert dealer.isReady() == True
      assert maker.getExposure() == 0
      assert taker.getExposure() == 0

      #check hedger can rebalance
      assert hedger.canRebalance() == False
      assert hedger.needsRebalance() == False

      #set maker index price
      await mockedConnection.setIndexPrice(10000)

      #check open volume
      vol = maker.getOpenVolume().get(5, 0)
      assert vol['ask'] == 1
      assert vol['bid'] == 1

      ## push an order ##
      await mockedConnection.push_new_order({
         'id' : 1,
         'timestamp' : 1,
         'quantity' : 0.3,
         'price' : 10000,
         'status' : ORDER_STATUS_PENDING,
         'reference_exposure' : 0,
         'session_id' : 5,
         'rollover_type' : ORDER_TYPE_TRADE_POSITION,
         'fee' : 4.5
      })

      #check exposure
      assert maker.getExposure() == 0.3
      assert taker.getExposure() == -0.3

      #check open volume, should reflect effect of exposure
      vol = maker.getOpenVolume().get(5, 0)
      assert vol['ask'] == 1.3
      assert vol['bid'] == 0.7

      ## push order on opposite side ##
      await mockedConnection.push_new_order({
         'id' : 2,
         'timestamp' : 1,
         'quantity' : -0.5,
         'price' : 10000,
         'status' : ORDER_STATUS_PENDING,
         'reference_exposure' : 0,
         'session_id' : 5,
         'rollover_type' : ORDER_TYPE_TRADE_POSITION,
         'fee' : 7.5
      })

      #check exposure
      assert maker.getExposure() == -0.2
      assert taker.getExposure() == 0.2

      #check open volume, should reflect effect of exposure
      vol = maker.getOpenVolume().get(5, 0)
      assert vol['ask'] == 0.8
      assert vol['bid'] == 1.2

      ## go flat at a higher price ##
      await mockedConnection.push_new_order({
         'id' : 3,
         'timestamp' : 1,
         'quantity' : 0.2,
         'price' : 10100,
         'status' : ORDER_STATUS_PENDING,
         'reference_exposure' : 0,
         'session_id' : 5,
         'rollover_type' : ORDER_TYPE_TRADE_POSITION,
         'fee' : 3
      })

      #check exposure
      assert maker.getExposure() == 0
      assert taker.getExposure() == 0

      '''
      Even though we have 0 exposure, some margin should still be
      stuck in the loss of the last position (at the time it was made):
       - pos1: 0 pnl
       - pos2: 0 pnl
       - pos3: -20 pnl
      '''
      vol = maker.getOpenVolume().get(5, 0)
      assert vol['ask'] == 0.98
      assert vol['bid'] == 1.0

      #move the index price
      await mockedConnection.setIndexPrice(10100)

      '''
      total pnl: -20
       - pos1: 30 pnl
       - pos2: -50 pnl
       - pos3: 0 pnl
      '''
      vol = maker.getOpenVolume().get(5, 0)
      assert vol['ask'] == 1.0
      assert vol['bid'] == 0.98

   #cover withdrawal code, triggered by hedger rebalancing
   @patch('leverex_core.base_client.AsyncApiConnection')
   async def test_rebalance_withdrawals(self, MockedLeverexConnObj):
      #return mocked leverex connection object instead of an instance
      #of leverex_core.api_connection.AsyncApiConnection
      mockedConnection = MockedLeverexConnectionClass(1000)
      MockedLeverexConnObj.return_value = mockedConnection

      #setup dealer
      maker = LeverexProvider(self.config)
      taker = TestTaker(startBalance=1500, addr="efgh")
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
      assert len(maker.balances) == 0
      await mockedConnection.pushBalanceUpdate()
      assert maker.balances['USDT'] == 1000

      #reply to session sub
      assert mockedConnection.session_product != None
      await mockedConnection.notifySessionOpen(
         5, #session_id
         10000, #open price
         0 #open timestamp
      )

      #check providers and hedger are ready
      assert maker.isReady() == True
      assert maker.isBroken() == False
      assert dealer.isReady() == True
      assert maker.getExposure() == 0
      assert taker.getExposure() == 0

      #check hedger can rebalance
      assert hedger.canRebalance() == True
      assert hedger.needsRebalance() == False

      #reduce taker balance, should trigger a withdrawal request from maker
      await taker.updateBalance(1000)
      cashMetrics = maker.getCashMetrics()
      assert taker.balance == 1000
      assert cashMetrics['total'] == 1000
      assert cashMetrics['pending'] == 0
      assert hedger.canRebalance() == True
      assert hedger.needsRebalance() == True
      assert len(mockedConnection.withdrawRequest) == 1
      assert mockedConnection.withdrawRequest[0]['amount'] == 200

      #ACK withdrawal request, should adjust maker balance, remove rebal target
      await mockedConnection.confirmWithdrawalRequest()
      assert taker.balance == 1000
      cashMetrics = maker.getCashMetrics()
      assert cashMetrics['total'] == 800
      assert cashMetrics['pending'] == 200
      assert hedger.canRebalance() == True
      assert hedger.needsRebalance() == True
      assert len(mockedConnection.withdrawRequest) == 0

      #push an order through, it shouldnt affect the rebalance target
      await mockedConnection.push_market_data(10000)
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

      assert taker.balance == 1000
      cashMetrics = maker.getCashMetrics()
      assert cashMetrics['total'] == 800
      assert cashMetrics['pending'] == 200
      assert hedger.canRebalance() == True
      assert hedger.needsRebalance() == True
      assert len(mockedConnection.withdrawRequest) == 0
      assert '0' in mockedConnection.pendingWtdr
      assert mockedConnection.pendingWtdr['0'].amount == '200.0'

      #complete withdrawal
      await mockedConnection.completeWithdrawalRequest()
      await taker.updateBalance(1200)
      cashMetrics = maker.getCashMetrics()
      assert taker.balance == 1200
      assert cashMetrics['total'] == 800
      assert cashMetrics['pending'] == 0
      assert hedger.canRebalance() == True
      assert hedger.needsRebalance() == False
      assert len(mockedConnection.pendingWtdr) == 0

   #cover withdrawal code, triggered by hedger rebalancing
   @patch('leverex_core.base_client.AsyncApiConnection')
   async def test_rebalance_withdrawals_with_cancel(self, MockedLeverexConnObj):
      #return mocked leverex connection object instead of an instance
      #of leverex_core.api_connection.AsyncApiConnection
      mockedConnection = MockedLeverexConnectionClass(1000)
      MockedLeverexConnObj.return_value = mockedConnection

      #setup dealer
      maker = LeverexProvider(self.config)
      taker = TestTaker(startBalance=1500, addr="efgh")
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
      assert len(maker.balances) == 0
      await mockedConnection.pushBalanceUpdate()
      assert maker.balances['USDT'] == 1000

      #reply to session sub
      assert mockedConnection.session_product != None
      await mockedConnection.notifySessionOpen(
         5, #session_id
         10000, #open price
         0 #open timestamp
      )

      #check providers and hedger are ready
      assert maker.isReady() == True
      assert maker.isBroken() == False
      assert dealer.isReady() == True
      assert maker.getExposure() == 0
      assert taker.getExposure() == 0

      #check hedger can rebalance
      assert hedger.canRebalance() == True
      assert hedger.needsRebalance() == False

      #send maker a pending withdrawal, will trigger a
      #rebalance with withdrawal cancellation
      wtd = WithdrawInfo({
         'id' : 20,
         'recv_address' : 'abcd',
         'currency' : 'USDT',
         'amount' : 500,
         'timestamp' : time.time(),
         'status' : 1
      })
      await mockedConnection.sendWithdrawalInfo(wtd)
      assert taker.balance == 1500
      cashMetrics = maker.getCashMetrics()
      assert cashMetrics['total'] == 1000
      assert cashMetrics['pending'] == 500

      assert hedger.canRebalance() == True
      assert hedger.needsRebalance() == True
      target = hedger.rebalMan.target
      assert target.maker.target == 1200
      assert target.taker.target == 1800
      assert target.maker.cancelPending['status'] == 'cancel_pending_ongoing'
      assert target.maker.toWithdraw['amount'] == 300
      assert target.maker.toWithdraw['status'] == 'withdraw_todo'
      assert '20' in mockedConnection.pendingWtdr

      #complete the cancellation request
      await mockedConnection.completeCancelRequest()
      assert taker.balance == 1500
      cashMetrics = maker.getCashMetrics()
      assert cashMetrics['total'] == 1500
      assert cashMetrics['pending'] == 0

      assert hedger.canRebalance() == True
      assert hedger.needsRebalance() == True
      target = hedger.rebalMan.target
      assert target.maker.target == 1200
      assert target.taker.target == 1800
      assert target.maker.cancelPending['status'] == 'cancel_pending_done'
      assert target.maker.toWithdraw['amount'] == 300
      assert target.maker.toWithdraw['status'] == 'withdraw_ongoing'
      assert len(mockedConnection.pendingWtdr) == 0

      #ACK withdrawal request
      await mockedConnection.confirmWithdrawalRequest()
      assert taker.balance == 1500
      cashMetrics = maker.getCashMetrics()
      assert cashMetrics['total'] == 1200
      assert cashMetrics['pending'] == 300
      assert hedger.canRebalance() == True
      assert hedger.needsRebalance() == True
      assert len(mockedConnection.withdrawRequest) == 0
      assert '0' in mockedConnection.pendingWtdr
      assert mockedConnection.pendingWtdr['0'].amount == '300.0'

      #complete withdrawal
      await mockedConnection.completeWithdrawalRequest()
      await taker.updateBalance(1800)
      cashMetrics = maker.getCashMetrics()
      assert taker.balance == 1800
      assert cashMetrics['total'] == 1200
      assert cashMetrics['pending'] == 0
      assert hedger.canRebalance() == True
      assert hedger.needsRebalance() == False
      assert len(mockedConnection.pendingWtdr) == 0
