import unittest
from decimal import Decimal
#import pdb; pdb.set_trace()

from leverex_core.utils import LeverexOrder, \
   ORDER_STATUS_FILLED, SIDE_BUY, SIDE_SELL, ORDER_TYPE_TRADE_POSITION, \
   SessionInfo, SessionOpenInfo, LeverexOpenVolume, SessionOrders, \
   ORDER_ACTION_CREATED, round_down
from Factories.Definitions import double_eq

################################################################################
##
#### Utils Tests
##
################################################################################
class MockedLeverexProvider(object):
   def __init__(self):
      self.balances = {}
      self.ccy = ""
      self.margin_ccy = ""
      self.indexPrice = 10000
      self.currentSession = SessionInfo(SessionOpenInfo({
         "product_type": "xbtusd_rf",
         "cut_off_at": 0,
         "last_cut_off_price": 10000,
         "session_id": 10,
         "previous_session_id": 0,
         "healthy": True,
         "fee_taker": 15,
         "fee_maker": -5
      }))

      self.orderData = {
         10: SessionOrders(10)
      }
      self.orderData[10].setSessionObj(self.currentSession)

class TestUtils(unittest.TestCase):
   def test_double_eq(self):
      assert double_eq(1, 1)
      assert double_eq(1, 1.00)
      assert not double_eq(1, -1)
      assert not double_eq(-1, 1)
      assert not double_eq(0, 0.1)
      assert not double_eq(-1.2, 0)

      assert double_eq(0.1, 0.1)
      assert not double_eq(0.1, -0.1)

      assert double_eq(1, 0.99999999)
      assert not double_eq(1, 0.9999)
      assert double_eq(1, 0.9999, 0.1)
      assert double_eq(0.9999, 1, 0.1)
      assert not double_eq(-0.9999, 1, 0.1)

   def testMargins(self):
      order1 = LeverexOrder({
         "id": 1234,
         "timestamp": 0,
         "quantity": 1,
         "price": 10000,
         "side": SIDE_BUY,
         "status": ORDER_STATUS_FILLED,
         "product_type": "xbtusd_rf",
         "reference_exposure": 0,
         "session_id": 10,
         "rollover_type": ORDER_TYPE_TRADE_POSITION,
         "fee": 0,
         "is_taker": True
      })

      order2 = LeverexOrder({
         "id": 1235,
         "timestamp": 0,
         "quantity": 1.3,
         "price": 10123.43,
         "side": SIDE_SELL,
         "status": ORDER_STATUS_FILLED,
         "product_type": "xbtusd_rf",
         "reference_exposure": 0,
         "session_id": 10,
         "rollover_type": ORDER_TYPE_TRADE_POSITION,
         "fee": 0,
         "is_taker": True
      })

      order3 = LeverexOrder({
         "id": 1236,
         "timestamp": 0,
         "quantity": 0.4,
         "price": 9420.69,
         "side": SIDE_BUY,
         "status": ORDER_STATUS_FILLED,
         "product_type": "xbtusd_rf",
         "reference_exposure": 0,
         "session_id": 10,
         "rollover_type": ORDER_TYPE_TRADE_POSITION,
         "fee": 0,
         "is_taker": True
      })

      order4 = LeverexOrder({
         "id": 1237,
         "timestamp": 0,
         "quantity": 0.6,
         "price": 9901.31,
         "side": SIDE_SELL,
         "status": ORDER_STATUS_FILLED,
         "product_type": "xbtusd_rf",
         "reference_exposure": 0,
         "session_id": 10,
         "rollover_type": ORDER_TYPE_TRADE_POSITION,
         "fee": 0,
         "is_taker": True
      })

      #no orders
      levP = MockedLeverexProvider()
      levOV = LeverexOpenVolume(levP)
      assert double_eq(levOV.getMargin(), 0)

      #1 order
      levP.orderData[10].setOrder(order1, ORDER_ACTION_CREATED)

      levOV = LeverexOpenVolume(levP)
      assert double_eq(levOV.getMargin(), 1000)

      #2 orders
      levP.orderData[10].setOrder(order2, ORDER_ACTION_CREATED)

      levOV = LeverexOpenVolume(levP)
      assert double_eq(levOV.getMargin(), 300)

      #3 orders
      levP.orderData[10].setOrder(order3, ORDER_ACTION_CREATED)

      levOV = LeverexOpenVolume(levP)
      assert double_eq(levOV.getMargin(), 100)

      #4 orders
      levP.orderData[10].setOrder(order4, ORDER_ACTION_CREATED)

      levOV = LeverexOpenVolume(levP)
      assert double_eq(levOV.getMargin(), 500)

   def testMax(self):
      order1 = LeverexOrder({
         "id": 1234,
         "timestamp": 0,
         "quantity": 0.3,
         "price": 10000,
         "side": SIDE_BUY,
         "status": ORDER_STATUS_FILLED,
         "product_type": "xbtusd_rf",
         "reference_exposure": 0,
         "session_id": 10,
         "rollover_type": ORDER_TYPE_TRADE_POSITION,
         "fee": 0,
         "is_taker": True
      })

      order2 = LeverexOrder({
         "id": 1235,
         "timestamp": 0,
         "quantity": 0.5,
         "price": 10000,
         "side": SIDE_SELL,
         "status": ORDER_STATUS_FILLED,
         "product_type": "xbtusd_rf",
         "reference_exposure": 0,
         "session_id": 10,
         "rollover_type": ORDER_TYPE_TRADE_POSITION,
         "fee": 0,
         "is_taker": True
      })

      order3 = LeverexOrder({
         "id": 1236,
         "timestamp": 0,
         "quantity": 0.2,
         "price": 10100,
         "side": SIDE_BUY,
         "status": ORDER_STATUS_FILLED,
         "product_type": "xbtusd_rf",
         "reference_exposure": 0,
         "session_id": 10,
         "rollover_type": ORDER_TYPE_TRADE_POSITION,
         "fee": 0,
         "is_taker": True
      })

      sessionIM = 1000
      levP = MockedLeverexProvider()
      levOV = LeverexOpenVolume(levP)

      balance = 1000

      #1 order
      levP.orderData[10].setOrder(order1, ORDER_ACTION_CREATED)
      levOV.openBalance = balance - levOV.getMargin()
      relBid, relAsk = levOV.getReleasableExposure(10000, 10000)

      assert str(relBid) == "0.70000001"
      assert str(relAsk) == "1.29999999"

      prjM = levOV.projectMargin(-relAsk, 10000, False)
      assert prjM == 1000

      #2 orders
      levP.orderData[10].setOrder(order2, ORDER_ACTION_CREATED)
      levOV.openBalance = balance - levOV.getMargin()
      relBid, relAsk = levOV.getReleasableExposure(10000, 10000)

      assert str(relBid) == "1.20000001"
      assert str(relAsk) == "0.79999999"

      prjM = levOV.projectMargin(relBid, 10000, False)
      assert prjM == 1000

      #3 orders
      levP.orderData[10].setOrder(order3, ORDER_ACTION_CREATED)
      levOV.openBalance = balance - levOV.getMargin()
      relBid, relAsk = levOV.getReleasableExposure(10000, 10000)

      assert str(relBid) == "1.00000001"
      assert str(relAsk) == "0.97999999"

      prjM = levOV.projectMargin(relBid, 10000, False)
      assert prjM == 1000

      relBid, relAsk = levOV.getReleasableExposure(10100, 10200)
      assert str(relBid) == "0.98000000"
      assert str(relAsk) == "0.99999999"

      prjM = levOV.projectMargin(relBid, 10200, False)
      assert str(prjM) == "999.999992"

   def testMaxAsk_EdgeCase(self):
      order1 = LeverexOrder({
         "id": 1234,
         "timestamp": 0,
         "quantity": 1.0,
         "price": 10500,
         "side": SIDE_SELL,
         "status": ORDER_STATUS_FILLED,
         "product_type": "xbtusd_rf",
         "reference_exposure": 0,
         "session_id": 10,
         "rollover_type": ORDER_TYPE_TRADE_POSITION,
         "fee": 0,
         "is_taker": True
      })

      order2 = LeverexOrder({
         "id": 1235,
         "timestamp": 0,
         "quantity": 0.23,
         "price": 10600.00,
         "side": SIDE_SELL,
         "status": ORDER_STATUS_FILLED,
         "product_type": "xbtusd_rf",
         "reference_exposure": 0,
         "session_id": 10,
         "rollover_type": ORDER_TYPE_TRADE_POSITION,
         "fee": 0,
         "is_taker": True
      })

      order3 = LeverexOrder({
         "id": 1236,
         "timestamp": 0,
         "quantity": 0.2,
         "price": 10750,
         "side": SIDE_BUY,
         "status": ORDER_STATUS_FILLED,
         "product_type": "xbtusd_rf",
         "reference_exposure": 0,
         "session_id": 10,
         "rollover_type": ORDER_TYPE_TRADE_POSITION,
         "fee": 0,
         "is_taker": True
      })

      sessionIM = 1000
      balances = {
         1150: { 'bid': 2.0062, 'ask': 0.09375 },
         1250: { 'bid': 2.1062, 'ask': 0.19791667 },
         1500: { 'bid': 2.3562, 'ask': 0.448 }
      }

      #setup
      levP = MockedLeverexProvider()
      levOV = LeverexOpenVolume(levP)

      #add orders
      levP.orderData[10].setOrder(order1, ORDER_ACTION_CREATED)
      levP.orderData[10].setOrder(order2, ORDER_ACTION_CREATED)
      levP.orderData[10].setOrder(order3, ORDER_ACTION_CREATED)
      assert double_eq(levOV.getMargin(), 1060)

      for balance in balances:
         levOV.openBalance = balance - levOV.getMargin()
         relBid, relAsk = levOV.getReleasableExposure(10640, 10660)

         maxOffers = balances[balance]
         assert double_eq(relBid, maxOffers['bid'])
         assert double_eq(relAsk, maxOffers['ask'])

         prjAsk = levOV.projectMargin(-relAsk, 10640, False)
         prjBid = levOV.projectMargin(relBid, 10660, False)

         assert double_eq(prjAsk, balance)
         assert double_eq(prjBid, balance)

   def testMaxBid_EdgeCase(self):
      order1 = LeverexOrder({
         "id": 1234,
         "timestamp": 0,
         "quantity": 1.0,
         "price": 9500,
         "side": SIDE_BUY,
         "status": ORDER_STATUS_FILLED,
         "product_type": "xbtusd_rf",
         "reference_exposure": 0,
         "session_id": 10,
         "rollover_type": ORDER_TYPE_TRADE_POSITION,
         "fee": 0,
         "is_taker": True
      })

      order2 = LeverexOrder({
         "id": 1235,
         "timestamp": 0,
         "quantity": 0.23,
         "price": 9400.00,
         "side": SIDE_BUY,
         "status": ORDER_STATUS_FILLED,
         "product_type": "xbtusd_rf",
         "reference_exposure": 0,
         "session_id": 10,
         "rollover_type": ORDER_TYPE_TRADE_POSITION,
         "fee": 0,
         "is_taker": True
      })

      order3 = LeverexOrder({
         "id": 1236,
         "timestamp": 0,
         "quantity": 0.2,
         "price": 9250,
         "side": SIDE_SELL,
         "status": ORDER_STATUS_FILLED,
         "product_type": "xbtusd_rf",
         "reference_exposure": 0,
         "session_id": 10,
         "rollover_type": ORDER_TYPE_TRADE_POSITION,
         "fee": 0,
         "is_taker": True
      })

      sessionIM = 1000
      balances = {
         1150: { 'ask': 2.0062, 'bid': 0.09375 },
         1250: { 'ask': 2.1062, 'bid': 0.19791667 },
         1500: { 'ask': 2.3562, 'bid': 0.448 }
      }

      #setup
      levP = MockedLeverexProvider()
      levOV = LeverexOpenVolume(levP)

      #add orders
      levP.orderData[10].setOrder(order1, ORDER_ACTION_CREATED)
      levP.orderData[10].setOrder(order2, ORDER_ACTION_CREATED)
      levP.orderData[10].setOrder(order3, ORDER_ACTION_CREATED)
      assert double_eq(levOV.getMargin(), 1060)

      for balance in balances:
         levOV.openBalance = balance - levOV.getMargin()
         relBid, relAsk = levOV.getReleasableExposure(9340, 9360)

         maxOffers = balances[balance]
         assert double_eq(relBid, maxOffers['bid'])
         assert double_eq(relAsk, maxOffers['ask'])

         prjAsk = levOV.projectMargin(-relAsk, 9340, False)
         prjBid = levOV.projectMargin(relBid, 9360, False)

         assert double_eq(prjAsk, balance)
         assert double_eq(prjBid, balance)
