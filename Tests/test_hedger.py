#import pdb; pdb.set_trace()
import unittest
import copy

from .utils import TestTaker, TestMaker, price
from leverex_core.utils import Order, SIDE_BUY, SIDE_SELL, \
   WithdrawInfo
from Factories.Definitions import Balance
from Hedger.SimpleHedger import SimpleHedger
from Factories.Dealer.Factory import DealerFactory

################################################################################
##
#### Hedger Tests
##
################################################################################
class TestHedger(unittest.IsolatedAsyncioTestCase):
   config = {}
   config['hedger'] = {
      'price_ratio' : 0.01,
      'max_offer_volume' : 5,
      'min_size' : 0.00006,
      'quote_ratio' : 0.2
   }
   config['rebalance'] = {
      'enable' : True,
      'threshold_pct' : 0.1,
      'min_amount' : 10
   }

   async def test_offers_signals(self):
      #test hedger making/pulling offers
      maker = TestMaker()
      taker = TestTaker()
      hedger = SimpleHedger(self.config)
      dealer = DealerFactory(maker, taker, hedger)
      await dealer.run()
      await dealer.waitOnReady()

      #set taker order book, we shouldn't generate offers until maker is ready
      await taker.updateBalance(15000)
      assert len(maker.offers) == 0

      await taker.populateOrderBook(10)
      assert len(maker.offers) == 0

      #setup maker
      await maker.updateBalance(10000)
      assert len(maker.offers) == 1

      #check the offers
      offers0 = maker.offers[0]
      assert len(offers0) == 1

      #shutdown maker, offers should be pulled
      await maker.setConnected(False)
      assert len(maker.offers) == 2

      #check the offers
      offers1 = maker.offers[1]
      assert len(offers1) == 0

      #restart maker, we should get offers once again
      await maker.setConnected(True)
      assert len(maker.offers) == 3

      #check the offers
      offers2 = maker.offers[2]
      assert len(offers2) == 1

      #shutdown taker, offers should be pulled
      await taker.setConnected(False)
      assert len(maker.offers) == 4

      #check the offers
      offers3 = maker.offers[3]
      assert len(offers3) == 0

      #shutdown maker, no offers should be added
      await maker.setConnected(False)
      assert len(maker.offers) == 4

   async def test_offers_volume(self):
      taker = TestTaker(startBalance=1500)
      maker = TestMaker(startBalance=1000)

      hedger = SimpleHedger(self.config)
      dealer = DealerFactory(maker, taker, hedger)
      await dealer.run()
      await dealer.waitOnReady()

      #we should have offers yet
      assert len(maker.offers) == 0

      #quote and check price & volumes of offers
      await taker.populateOrderBook(10)
      assert len(maker.offers) == 1

      offers0 = maker.offers[0]
      assert len(offers0) == 1
      assert offers0[0].volume == 0.8
      assert offers0[0].bid == round(9989.58  * 0.99, 2)
      assert offers0[0].ask == round(10010.42 * 1.01, 2)

      #balance event
      await maker.updateBalance(500)
      assert len(maker.offers) == 2

      offers1 = maker.offers[1]
      assert len(offers1) == 1
      assert offers1[0].volume == 0.4
      assert offers1[0].bid == round(9989.58  * 0.99, 2)
      assert offers1[0].ask == round(10010.42 * 1.01, 2)

      #order book event
      await taker.populateOrderBook(6)
      assert len(maker.offers) == 3

      offers2 = maker.offers[2]
      assert len(offers2) == 1
      assert offers2[0].volume == 0.4
      assert offers2[0].bid == round(9993.75  * 0.99, 2)
      assert offers2[0].ask == round(10006.25 * 1.01, 2)

   async def test_offers_order(self):
      #maker orders should affect maker and taker exposure accordingly
      #effect of order should be reflected on margins, and on offers
      taker = TestTaker(startBalance=1500)
      maker = TestMaker(startBalance=500)

      hedger = SimpleHedger(self.config)
      dealer = DealerFactory(maker, taker, hedger)
      await dealer.run()
      await dealer.waitOnReady()
      assert len(maker.offers) == 0

      #order book event
      await taker.populateOrderBook(6)
      assert len(maker.offers) == 1

      offers0 = maker.offers[0]
      assert len(offers0) == 1
      assert offers0[0].volume == 0.4
      assert offers0[0].bid == round(9993.75  * 0.99, 2)
      assert offers0[0].ask == round(10006.25 * 1.01, 2)

      #new order event
      newOrder = Order(id=1, timestamp=0, quantity=0.1, price=10100, side=SIDE_BUY)
      await maker.newOrder(newOrder)
      assert len(maker.offers) == 2

      #check exposure
      assert maker.getExposure() == 0.1
      assert taker.getExposure() == -0.1

      #check volumes
      openVolume = maker.getOpenVolume()
      makerVolume = maker.getOpenVolume().get(
         self.config['hedger']['max_offer_volume'],
         self.config['hedger']['quote_ratio'])
      assert makerVolume['ask'] == 0.52
      assert makerVolume['bid'] == 0.32

      takerVolume = taker.getOpenVolume().get(
         self.config['hedger']['max_offer_volume'],
         self.config['hedger']['quote_ratio'])
      assert takerVolume['ask'] == 0.72
      assert takerVolume['bid'] == 0.92

      #check offers
      offers1 = maker.offers[1]
      assert len(offers1) == 2

      assert offers1[0].volume == 0.52
      assert offers1[0].bid == None
      assert offers1[0].ask == round(10006.25 * 1.01, 2)

      assert offers1[1].volume == 0.32
      assert offers1[1].bid == round(9993.75  * 0.99, 2)
      assert offers1[1].ask == None

   #exposure tests set various exposure on the maker and the taker,
   #then start the dealer and check the exposures are in sync
   async def test_exposure_sync_maker(self):
      #setup taker and maker
      taker = TestTaker(startBalance=1500)

      startOrders = []
      startOrders.append(Order(id=1, timestamp=0, quantity=0.1, price=10100, side=SIDE_BUY))
      startOrders.append(Order(id=2, timestamp=0, quantity=0.2, price=10150, side=SIDE_BUY))
      maker = TestMaker(startBalance=1000, startPositions=startOrders)

      #check they have no balance nor exposure pre dealer start
      assert maker.getExposure() == None
      assert taker.getExposure() == None
      assert maker.balance == 0
      assert taker.balance == 0

      hedger = SimpleHedger(self.config)
      dealer = DealerFactory(maker, taker, hedger)
      await dealer.run()
      await dealer.waitOnReady()

      #check taker exposure is the opposite of the maker's
      assert maker.balance == 1000
      assert taker.balance == 1500
      assert maker.getExposure() == 0.3
      assert taker.getExposure() == -0.3

      #add another order
      newOrder = Order(id=3, timestamp=0, quantity=-0.1, price=9900, side=SIDE_SELL)
      await maker.newOrder(newOrder)

      assert maker.balance == 1000
      assert taker.balance == 1500
      assert maker.getExposure() == 0.2
      assert taker.getExposure() == -0.2

   async def test_exposure_sync_taker(self):
      #setup taker and maker
      taker = TestTaker(startBalance=1500, startExposure=0.25)
      maker = TestMaker(startBalance=1000)

      #check they have no balance nor exposure pre dealer start
      assert maker.getExposure() == None
      assert taker.getExposure() == None
      assert maker.balance == 0
      assert taker.balance == 0

      hedger = SimpleHedger(self.config)
      dealer = DealerFactory(maker, taker, hedger)
      await dealer.run()
      await dealer.waitOnReady()

      #check taker exposure is zero'd out since maker has none
      assert maker.balance == 1000
      assert taker.balance == 1500
      assert maker.getExposure() == 0
      assert taker.getExposure() == 0

      #add another order
      newOrder = Order(id=3, timestamp=0, quantity=0.1, price=9900, side=SIDE_BUY)
      await maker.newOrder(newOrder)

      assert maker.balance == 1000
      assert taker.balance == 1500
      assert maker.getExposure() == 0.1
      assert taker.getExposure() == -0.1

   async def test_exposure_sync_both(self):
      #setup taker and maker
      taker = TestTaker(startBalance=1500, startExposure=0.5)

      startOrders = []
      startOrders.append(Order(id=1, timestamp=0, quantity=0.3, price=10100, side=SIDE_BUY))
      startOrders.append(Order(id=2, timestamp=0, quantity=0.1, price=10150, side=SIDE_BUY))
      maker = TestMaker(startBalance=1000, startPositions=startOrders)

      #check they have no balance nor exposure pre dealer start
      assert maker.getExposure() == None
      assert taker.getExposure() == None
      assert maker.balance == 0
      assert taker.balance == 0

      hedger = SimpleHedger(self.config)
      dealer = DealerFactory(maker, taker, hedger)
      await dealer.run()
      await dealer.waitOnReady()

      #check taker exposure matches maker
      assert maker.balance == 1000
      assert taker.balance == 1500
      assert maker.getExposure() == 0.4
      assert taker.getExposure() == -0.4

      #add another order
      newOrder = Order(id=3, timestamp=0, quantity=-0.1, price=9900, side=SIDE_SELL)
      await maker.newOrder(newOrder)

      assert maker.balance == 1000
      assert taker.balance == 1500
      assert maker.getExposure() == 0.3
      assert taker.getExposure() == -0.3

   async def test_broken_provider(self):
      #setup taker and maker
      taker = TestTaker(startBalance=1500, startExposure=0.5)

      startOrders = []
      startOrders.append(Order(id=1, timestamp=0, quantity=0.3, price=10100, side=SIDE_BUY))
      startOrders.append(Order(id=2, timestamp=0, quantity=0.1, price=10150, side=SIDE_BUY))
      maker = TestMaker(startBalance=1000, startPositions=startOrders)

      #check they have no balance nor exposure pre dealer start
      assert maker.getExposure() == None
      assert taker.getExposure() == None
      assert maker.balance == 0
      assert taker.balance == 0

      hedger = SimpleHedger(self.config)
      dealer = DealerFactory(maker, taker, hedger)
      await dealer.run()
      await dealer.waitOnReady()

      assert maker.isReady() == True
      assert maker.isBroken() == False
      assert taker.isReady() == True
      assert hedger.isReady() == True
      assert dealer.isReady() == True

      #check taker exposure matches maker
      assert maker.balance == 1000
      assert taker.balance == 1500
      assert maker.getExposure() == 0.4
      assert taker.getExposure() == -0.4

      #stop the maker, taker exposure shouldn't change
      await maker.setExplicitState(False)

      assert maker.isReady() == False
      assert maker.isBroken() == False
      assert taker.isReady() == True
      assert hedger.isReady() == True
      assert dealer.isReady() == False

      assert maker.getExposure() == None
      assert taker.getExposure() == -0.4

      #restart the maker, taker exposure shouldn't change
      await maker.setExplicitState(True)

      assert maker.isReady() == True
      assert maker.isBroken() == False
      assert taker.isReady() == True
      assert hedger.isReady() == True
      assert dealer.isReady() == True

      assert maker.getExposure() == 0.4
      assert taker.getExposure() == -0.4

      #break the maker, taker exposure should go to 0
      await maker.explicitBreak()

      assert maker.isReady() == False
      assert maker.isBroken() == True
      assert taker.isReady() == True
      assert hedger.isReady() == True
      assert dealer.isReady() == False

      assert maker.getExposure() == None
      assert taker.getExposure() == 0

   async def test_liquidation_target(self):
      #setup taker and maker
      taker = TestTaker(startBalance=1500)
      maker = TestMaker(startBalance=1000)

      #check they have no balance nor exposure pre dealer start
      assert maker.getExposure() == None
      assert taker.getExposure() == None
      assert maker.balance == 0
      assert taker.balance == 0

      hedger = SimpleHedger(self.config)
      dealer = DealerFactory(maker, taker, hedger)
      await dealer.run()
      await dealer.waitOnReady()

      assert maker.isReady() == True
      assert maker.isBroken() == False
      assert taker.isReady() == True
      assert hedger.isReady() == True
      assert dealer.isReady() == True

      #check taker exposure matches maker
      assert maker.balance == 1000
      assert taker.balance == 1500
      assert maker.getExposure() == 0
      assert taker.getExposure() == 0

      await maker.setOpenPrice(10000)
      assert taker.targetCollateral == None

      newOrder = Order(id=3, timestamp=0, quantity=0.5, price=9900, side=SIDE_BUY)
      await maker.newOrder(newOrder)

      assert maker.getExposure() == 0.5
      assert taker.getExposure() == -0.5
      assert taker.targetCollateral == 750

      newOrder = Order(id=4, timestamp=0, quantity=-0.9, price=9900, side=SIDE_SELL)
      await maker.newOrder(newOrder)

      assert maker.getExposure() == -0.4
      assert taker.getExposure() == 0.4
      assert taker.targetCollateral == 600

      await maker.setOpenPrice(10100)
      assert taker.targetCollateral == 606

   async def test_rebalance_target(self):
      #setup taker and maker
      taker = TestTaker(startBalance=1500)
      maker = TestMaker(startBalance=1000)

      #check they have no balance nor exposure pre dealer start
      assert maker.getExposure() == None
      assert taker.getExposure() == None
      assert maker.balance == 0
      assert taker.balance == 0

      hedger = SimpleHedger(self.config)
      dealer = DealerFactory(maker, taker, hedger)
      await dealer.run()
      await dealer.waitOnReady()

      assert maker.isReady() == True
      assert maker.isBroken() == False
      assert taker.isReady() == True
      assert hedger.isReady() == True
      assert dealer.isReady() == True

      #check rebalance target is same as balance
      assert maker.balance == 1000
      assert taker.balance == 1500
      assert maker.getExposure() == 0
      assert taker.getExposure() == 0
      assert hedger.rebalMan.canAssess() == True
      assert hedger.rebalMan.canWithdraw() == False
      assert hedger.canRebalance() == False
      target = hedger.rebalMan.target
      assert target.maker.target == 1000
      assert target.taker.target == 1500
      assert target.maker.toWithdraw['amount'] == 0

      #reduce taker cash, maker rebalance target should go up
      await taker.updateBalance(1200)
      assert maker.balance == 1000
      assert taker.balance == 1200
      assert maker.getExposure() == 0
      assert taker.getExposure() == 0
      assert hedger.rebalMan.canAssess() == True
      target = hedger.rebalMan.target
      assert target.maker.target == 880
      assert target.taker.target == 1320
      assert target.maker.toWithdraw['amount'] == 120

      #post an order, rebalance target shouldn't change
      await maker.newOrder(Order(
         id=1, timestamp=0, quantity=0.5, price=10100, side=SIDE_BUY))
      assert maker.balance == 1000
      assert taker.balance == 1200
      assert maker.getExposure() == 0.5
      assert taker.getExposure() == -0.5
      assert hedger.rebalMan.canAssess() == True
      assert target == hedger.rebalMan.target
      assert target.maker.target == 880
      assert target.taker.target == 1320
      assert target.maker.toWithdraw['amount'] == 120

      #increase maker balance to 50 coins worth of volume
      await maker.updateBalance(50000)
      assert maker.balance == 50000
      assert taker.balance == 1200
      assert maker.getExposure() == 0.5
      assert taker.getExposure() == -0.5
      assert hedger.rebalMan.canAssess() == True
      assert target != hedger.rebalMan.target
      target = hedger.rebalMan.target
      assert target.maker.target == 20480
      assert target.taker.target == 30720
      assert target.maker.toWithdraw['amount'] == 29520

      #reduce balances near 1.5x max volume
      await taker.updateBalance(20000)
      assert maker.balance == 50000
      assert taker.balance == 20000
      assert maker.getExposure() == 0.5
      assert taker.getExposure() == -0.5
      assert hedger.rebalMan.canAssess() == True
      target = hedger.rebalMan.target
      assert target.maker.target == 28000
      assert target.taker.target == 42000
      assert target.maker.toWithdraw['amount'] == 22000

      await maker.updateBalance(15000)
      assert maker.balance == 15000
      assert taker.balance == 20000
      assert maker.getExposure() == 0.5
      assert taker.getExposure() == -0.5
      assert hedger.rebalMan.canAssess() == True
      target = hedger.rebalMan.target
      assert target.maker.target == 14000
      assert target.taker.target == 21000
      assert target.maker.toWithdraw['amount'] == 1000

      #set taker above maker
      await maker.updateBalance(10000)
      assert maker.balance == 10000
      assert taker.balance == 20000
      assert maker.getExposure() == 0.5
      assert taker.getExposure() == -0.5
      assert hedger.rebalMan.canAssess() == True
      target = hedger.rebalMan.target
      assert target.maker.target == 12000
      assert target.taker.target == 18000
      assert target.maker.toWithdraw['amount'] == 0
      assert target.taker.toWithdraw['amount'] == 2000

   async def test_rebalance_target_with_pending(self):
      localConfig = copy.deepcopy(self.config)
      localConfig['rebalance']['threshold_pct'] = 0.02

      #setup taker and maker
      taker = TestTaker(startBalance=1500)
      maker = TestMaker(startBalance=1000, pendingWithdrawals=[200])

      #check they have no balance nor exposure pre dealer start
      assert maker.getExposure() == None
      assert taker.getExposure() == None
      assert maker.balance == 0
      assert taker.balance == 0

      hedger = SimpleHedger(localConfig)
      dealer = DealerFactory(maker, taker, hedger)
      await dealer.run()
      await dealer.waitOnReady()

      assert maker.isReady() == True
      assert maker.isBroken() == False
      assert taker.isReady() == True
      assert hedger.isReady() == True
      assert dealer.isReady() == True

      #check rebalance target is same as balance
      assert maker.balance == 1000
      assert taker.balance == 1500
      assert maker.getExposure() == 0
      assert taker.getExposure() == 0
      assert hedger.canRebalance() == False
      assert hedger.rebalMan.canAssess() == True
      target = hedger.rebalMan.target
      assert target.maker.target == 1080
      assert target.taker.target == 1620
      assert target.maker.cancelPending['status'] == 'cancel_pending_ongoing'
      assert target.maker.toWithdraw['amount'] == 120

      #reduce taker cash, maker rebalance target should go up
      await taker.updateBalance(1200)
      assert maker.balance == 1000
      assert taker.balance == 1200
      assert maker.getExposure() == 0
      assert taker.getExposure() == 0
      assert hedger.rebalMan.canAssess() == True
      target = hedger.rebalMan.target
      assert target.maker.target == 960
      assert target.taker.target == 1440
      assert target.maker.cancelPending['status'] == 'cancel_pending_ongoing'
      assert target.maker.toWithdraw['amount'] == 240

      #post an order, rebalance target shouldn't change
      await maker.newOrder(Order(
         id=1, timestamp=0, quantity=0.5, price=10100, side=SIDE_BUY))
      assert maker.balance == 1000
      assert taker.balance == 1200
      assert maker.getExposure() == 0.5
      assert taker.getExposure() == -0.5
      assert hedger.rebalMan.canAssess() == True
      assert target == hedger.rebalMan.target

      #increase maker balance to 50 coins worth of volume
      await maker.updateBalance(50000)
      assert maker.balance == 50000
      assert taker.balance == 1200
      assert maker.getExposure() == 0.5
      assert taker.getExposure() == -0.5
      assert hedger.rebalMan.canAssess() == True
      target = hedger.rebalMan.target
      assert target.maker.target == 20560
      assert target.taker.target == 30840
      assert target.maker.cancelPending['status'] == 'cancel_pending_ongoing'
      assert target.maker.toWithdraw['amount'] == 29640

      #reduce balances near 1.5x max volume
      await taker.updateBalance(20000)
      assert maker.balance == 50000
      assert taker.balance == 20000
      assert maker.getExposure() == 0.5
      assert taker.getExposure() == -0.5
      assert hedger.rebalMan.canAssess() == True
      target = hedger.rebalMan.target
      assert target.maker.target == 28080
      assert target.taker.target == 42120
      assert target.maker.cancelPending['status'] == 'cancel_pending_ongoing'
      assert target.maker.toWithdraw['amount'] == 22120

      await maker.updateBalance(15000)
      assert maker.balance == 15000
      assert taker.balance == 20000
      assert maker.getExposure() == 0.5
      assert taker.getExposure() == -0.5
      assert hedger.rebalMan.canAssess() == True
      target = hedger.rebalMan.target
      assert target.maker.target == 14080
      assert target.taker.target == 21120
      assert target.maker.cancelPending['status'] == 'cancel_pending_ongoing'
      assert target.maker.toWithdraw['amount'] == 1120

      #set taker above maker
      await maker.updateBalance(10000)
      assert maker.balance == 10000
      assert taker.balance == 20000
      assert maker.getExposure() == 0.5
      assert taker.getExposure() == -0.5
      target = hedger.rebalMan.target
      assert target.maker.target == 12080
      assert target.taker.target == 18120
      assert target.maker.cancelPending['status'] == 'cancel_pending_ongoing'
      assert target.maker.toWithdraw['amount'] == 0
      assert target.taker.toWithdraw['amount'] == 1880

   async def test_rebalance_target_with_withdraw(self):
      #setup taker and maker
      taker = TestTaker(startBalance=1500, addr="efgh")
      maker = TestMaker(startBalance=1000)

      #check they have no balance nor exposure pre dealer start
      assert maker.getExposure() == None
      assert taker.getExposure() == None
      assert maker.balance == 0
      assert taker.balance == 0

      hedger = SimpleHedger(self.config)
      dealer = DealerFactory(maker, taker, hedger)
      await dealer.run()
      await dealer.waitOnReady()

      assert maker.isReady() == True
      assert maker.isBroken() == False
      assert taker.isReady() == True
      assert hedger.isReady() == True
      assert hedger.canRebalance() == True
      assert dealer.isReady() == True

      #check rebalance target is same as balance
      assert maker.balance == 1000
      assert taker.balance == 1500
      assert maker.getExposure() == 0
      assert taker.getExposure() == 0
      assert hedger.rebalMan.canAssess() == True
      assert hedger.rebalMan.canWithdraw() == True
      assert hedger.canRebalance() == True
      target = hedger.rebalMan.target
      assert target.maker.target == 1000
      assert target.taker.target == 1500
      assert target.maker.toWithdraw['amount'] == 0
      assert len(maker.withdrawalHist) == 0

      #reduce taker cash, maker rebalance target should go up
      await taker.updateBalance(1000)
      assert maker.balance == 1000
      assert taker.balance == 1000
      assert maker.getExposure() == 0
      assert taker.getExposure() == 0
      assert hedger.rebalMan.canAssess() == False
      assert hedger.rebalMan.canWithdraw() == True
      assert hedger.canRebalance() == True
      target = hedger.rebalMan.target
      assert target.maker.target == 800
      assert target.taker.target == 1200
      assert target.maker.toWithdraw['amount'] == 200
      assert len(maker.withdrawalHist) == 0

      #ACK maker withdrawal request
      await maker.pushWithdrawal()
      assert maker.balance == 800
      assert taker.balance == 1000
      assert maker.getExposure() == 0
      assert taker.getExposure() == 0
      assert hedger.rebalMan.canAssess() == False
      assert target == hedger.rebalMan.target
      assert len(maker.withdrawalHist) == 1
      assert maker.withdrawalHist[0]['amount'] == 200

      #complete rebalance by funding taker the cash
      await maker.completeWithdrawal(taker)
      assert maker.balance == 800
      assert taker.balance == 1200
      assert maker.getExposure() == 0
      assert taker.getExposure() == 0
      assert hedger.rebalMan.canAssess() == True
      assert hedger.rebalMan.target == None
      assert len(maker.withdrawalHist) == 1
      assert maker.withdrawalHist[0]['amount'] == 200

      #give maker more cash
      await maker.updateBalance(1000)
      assert maker.balance == 1000
      assert taker.balance == 1200
      assert maker.getExposure() == 0
      assert taker.getExposure() == 0
      assert hedger.rebalMan.canAssess() == True
      target = hedger.rebalMan.target
      assert target.maker.target == 880
      assert target.taker.target == 1320
      assert target.maker.toWithdraw['amount'] == 120
      assert len(maker.withdrawalHist) == 1
      assert maker.withdrawalHist[0]['amount'] == 200

      #give maker more cash
      await maker.updateBalance(1200)
      assert maker.balance == 1200
      assert taker.balance == 1200
      assert maker.getExposure() == 0
      assert taker.getExposure() == 0
      assert hedger.rebalMan.canAssess() == False
      target = hedger.rebalMan.target
      assert target.maker.target == 960
      assert target.taker.target == 1440
      assert target.maker.toWithdraw['amount'] == 240
      assert len(maker.withdrawalHist) == 1
      assert maker.withdrawalHist[0]['amount'] == 200

      #ACK maker withdrawal request
      await maker.pushWithdrawal()
      assert maker.balance == 960
      assert taker.balance == 1200
      assert maker.getExposure() == 0
      assert taker.getExposure() == 0
      assert hedger.rebalMan.canAssess() == False
      assert target == hedger.rebalMan.target
      assert len(maker.withdrawalHist) == 2
      assert maker.withdrawalHist[0]['amount'] == 200
      assert maker.withdrawalHist[1]['amount'] == 240

   async def test_rebalance_trigger_again(self):
      #setup taker and maker
      taker = TestTaker(startBalance=1500, addr="efgh")
      maker = TestMaker(startBalance=1000)

      #check they have no balance nor exposure pre dealer start
      assert maker.getExposure() == None
      assert taker.getExposure() == None
      assert maker.balance == 0
      assert taker.balance == 0

      hedger = SimpleHedger(self.config)
      dealer = DealerFactory(maker, taker, hedger)
      await dealer.run()
      await dealer.waitOnReady()

      assert maker.isReady() == True
      assert maker.isBroken() == False
      assert taker.isReady() == True
      assert hedger.isReady() == True
      assert hedger.canRebalance() == True
      assert dealer.isReady() == True

      #check rebalance target is same as balance
      assert maker.balance == 1000
      assert taker.balance == 1500
      assert maker.getExposure() == 0
      assert taker.getExposure() == 0
      assert hedger.rebalMan.canAssess() == True
      assert hedger.rebalMan.canWithdraw() == True
      assert hedger.canRebalance() == True
      target = hedger.rebalMan.target
      assert target.maker.target == 1000
      assert target.taker.target == 1500
      assert target.maker.toWithdraw['amount'] == 0
      assert len(maker.withdrawalHist) == 0

      #reduce taker cash, maker rebalance target should go up
      await taker.updateBalance(1000)
      assert maker.balance == 1000
      assert taker.balance == 1000
      assert maker.getExposure() == 0
      assert taker.getExposure() == 0
      assert hedger.rebalMan.canAssess() == False
      assert hedger.rebalMan.canWithdraw() == True
      assert hedger.canRebalance() == True
      target = hedger.rebalMan.target
      assert target.maker.target == 800
      assert target.taker.target == 1200
      assert target.maker.toWithdraw['amount'] == 200
      assert len(maker.withdrawalHist) == 0
      assert len(maker.withdrawalsToPush) == 1

      #ACK maker withdrawal request, update taker cash
      await maker.pushWithdrawal()
      await maker.completeWithdrawal(taker)
      assert maker.balance == 800
      assert taker.balance == 1200
      assert maker.getExposure() == 0
      assert taker.getExposure() == 0
      assert hedger.rebalMan.canAssess() == True
      assert hedger.rebalMan.canWithdraw() == True
      assert hedger.canRebalance() == True
      assert hedger.rebalMan.target == None
      assert len(maker.withdrawalHist) == 1
      assert maker.withdrawalHist[0]['amount'] == 200
      assert len(maker.withdrawalsToPush) == 0

      #trigger a balance event, the target amount should now be 0
      await dealer.onEvent(None, Balance)
      assert maker.balance == 800
      assert taker.balance == 1200
      assert maker.getExposure() == 0
      assert taker.getExposure() == 0
      assert hedger.rebalMan.canAssess() == True
      assert hedger.rebalMan.canWithdraw() == True
      assert hedger.canRebalance() == True
      target = hedger.rebalMan.target
      assert target.maker.target == 800
      assert target.taker.target == 1200
      assert target.maker.toWithdraw['amount'] == 0
      assert len(maker.withdrawalHist) == 1
      assert maker.withdrawalHist[0]['amount'] == 200
      assert len(maker.withdrawalsToPush) == 0

   async def test_rebalance_target_with_maker_cancellation(self):
      localConfig = copy.deepcopy(self.config)
      localConfig['rebalance']['threshold_pct'] = 0.02

      #setup taker and maker
      taker = TestTaker(startBalance=2000, addr="efgh")
      maker = TestMaker(startBalance=800, pendingWithdrawals=[200])

      #check they have no balance nor exposure pre dealer start
      assert maker.getExposure() == None
      assert taker.getExposure() == None
      assert maker.balance == 0
      assert taker.balance == 0

      hedger = SimpleHedger(localConfig)
      dealer = DealerFactory(maker, taker, hedger)
      await dealer.run()
      await dealer.waitOnReady()

      assert maker.isReady() == True
      assert maker.isBroken() == False
      assert taker.isReady() == True
      assert hedger.isReady() == True
      assert hedger.canRebalance() == True
      assert dealer.isReady() == True

      #rebalance target should request to cancel maker withdrawal
      assert maker.balance == 800
      assert taker.balance == 2000
      assert maker.getExposure() == 0
      assert taker.getExposure() == 0
      assert hedger.rebalMan.canAssess() == False
      assert hedger.rebalMan.canWithdraw() == True
      assert hedger.canRebalance() == True
      target = hedger.rebalMan.target
      assert target.maker.target == 1200
      assert target.taker.target == 1800
      assert target.maker.toWithdraw['amount'] == 0
      assert target.taker.toWithdraw['amount'] == 200
      assert len(maker.withdrawalHist) == 1
      assert len(taker.withdrawalsToPush) == 0
      assert maker.withdrawalHist[0]['status'] == WithdrawInfo.WITHDRAW_PENDING
      assert maker.cancelWithdrawalsRequested != None

      #complete maker withdrawal cancellation
      await maker.completeWithdrawCancellation()
      assert maker.balance == 1000
      assert taker.balance == 2000
      assert maker.getExposure() == 0
      assert taker.getExposure() == 0
      assert hedger.rebalMan.canAssess() == False
      assert hedger.rebalMan.canWithdraw() == True
      assert hedger.canRebalance() == True
      target = hedger.rebalMan.target
      assert target.maker.target == 1200
      assert target.taker.target == 1800
      assert target.maker.toWithdraw['amount'] == 0
      assert target.taker.toWithdraw['amount'] == 200
      assert len(maker.withdrawalHist) == 1
      assert len(taker.withdrawalsToPush) == 1
      assert maker.withdrawalHist[0]['status'] == WithdrawInfo.WITHDRAW_CANCELLED
      assert maker.cancelWithdrawalsRequested == None

      #complete taker withdrawal
      await taker.pushWithdrawal()
      assert maker.balance == 1000
      assert taker.balance == 1800
      assert maker.getExposure() == 0
      assert taker.getExposure() == 0
      assert hedger.rebalMan.canAssess() == False
      assert hedger.rebalMan.canWithdraw() == True
      assert hedger.canRebalance() == True
      target = hedger.rebalMan.target
      assert target.maker.target == 1200
      assert target.taker.target == 1800
      assert target.maker.toWithdraw['amount'] == 0
      assert target.taker.toWithdraw['amount'] == 200
      assert len(maker.withdrawalHist) == 1
      assert len(taker.withdrawalHist) == 1
      assert maker.withdrawalHist[0]['status'] == WithdrawInfo.WITHDRAW_CANCELLED
      assert taker.withdrawalHist[0]['status'] == WithdrawInfo.WITHDRAW_BROADCASTED
      assert taker.withdrawalHist[0]['amount'] == 200

      #finally, send cash to maker
      await taker.completeWithdrawal(maker)
      assert maker.balance == 1200
      assert taker.balance == 1800
      assert maker.getExposure() == 0
      assert taker.getExposure() == 0
      assert hedger.rebalMan.canAssess() == True
      assert hedger.rebalMan.canWithdraw() == True
      assert hedger.canRebalance() == True
      assert hedger.rebalMan.target == None
      assert target.maker.toWithdraw['amount'] == 0
      assert target.taker.toWithdraw['amount'] == 200
      assert taker.withdrawalHist[0]['status'] == WithdrawInfo.WITHDRAW_COMPLETED

   async def test_rebalance_target_with_taker_cancellation(self):
      localConfig = copy.deepcopy(self.config)
      localConfig['rebalance']['threshold_pct'] = 0.02

      #setup taker and maker
      taker = TestTaker(startBalance=1200, addr="efgh", pendingWithdrawals=[400])
      maker = TestMaker(startBalance=1400)

      #check they have no balance nor exposure pre dealer start
      assert maker.getExposure() == None
      assert taker.getExposure() == None
      assert maker.balance == 0
      assert taker.balance == 0

      hedger = SimpleHedger(localConfig)
      dealer = DealerFactory(maker, taker, hedger)
      await dealer.run()
      await dealer.waitOnReady()

      assert maker.isReady() == True
      assert maker.isBroken() == False
      assert taker.isReady() == True
      assert hedger.isReady() == True
      assert hedger.canRebalance() == True
      assert dealer.isReady() == True

      #rebalance target should request to cancel maker withdrawal
      assert maker.balance == 1400
      assert taker.balance == 1200
      assert maker.getExposure() == 0
      assert taker.getExposure() == 0
      assert hedger.rebalMan.canAssess() == False
      assert hedger.rebalMan.canWithdraw() == True
      assert hedger.canRebalance() == True
      target = hedger.rebalMan.target
      assert target.maker.target == 1200
      assert target.taker.target == 1800
      assert target.maker.toWithdraw['amount'] == 200
      assert target.taker.toWithdraw['amount'] == 0
      assert len(taker.withdrawalHist) == 1
      assert len(maker.withdrawalsToPush) == 0
      assert taker.withdrawalHist[0]['status'] == WithdrawInfo.WITHDRAW_PENDING
      assert taker.cancelWithdrawalsRequested != None

      #complete maker withdrawal cancellation
      await taker.completeWithdrawCancellation()
      assert maker.balance == 1400
      assert taker.balance == 1600
      assert maker.getExposure() == 0
      assert taker.getExposure() == 0
      assert hedger.rebalMan.canAssess() == False
      assert hedger.rebalMan.canWithdraw() == True
      assert hedger.canRebalance() == True
      target = hedger.rebalMan.target
      assert target.maker.target == 1200
      assert target.taker.target == 1800
      assert target.taker.toWithdraw['amount'] == 0
      assert target.maker.toWithdraw['amount'] == 200
      assert len(taker.withdrawalHist) == 1
      assert len(maker.withdrawalsToPush) == 1
      assert taker.withdrawalHist[0]['status'] == WithdrawInfo.WITHDRAW_CANCELLED
      assert taker.cancelWithdrawalsRequested == None

      #complete taker withdrawal
      await maker.pushWithdrawal()
      assert maker.balance == 1200
      assert taker.balance == 1600
      assert maker.getExposure() == 0
      assert taker.getExposure() == 0
      assert hedger.rebalMan.canAssess() == False
      assert hedger.rebalMan.canWithdraw() == True
      assert hedger.canRebalance() == True
      target = hedger.rebalMan.target
      assert target.maker.target == 1200
      assert target.taker.target == 1800
      assert target.taker.toWithdraw['amount'] == 0
      assert target.maker.toWithdraw['amount'] == 200
      assert len(maker.withdrawalHist) == 1
      assert len(taker.withdrawalHist) == 1
      assert taker.withdrawalHist[0]['status'] == WithdrawInfo.WITHDRAW_CANCELLED
      assert maker.withdrawalHist[0]['status'] == WithdrawInfo.WITHDRAW_BROADCASTED
      assert maker.withdrawalHist[0]['amount'] == 200

      #finally, send cash to maker
      await maker.completeWithdrawal(taker)
      assert maker.balance == 1200
      assert taker.balance == 1800
      assert maker.getExposure() == 0
      assert taker.getExposure() == 0
      assert hedger.rebalMan.canAssess() == True
      assert hedger.rebalMan.canWithdraw() == True
      assert hedger.canRebalance() == True
      assert hedger.rebalMan.target == None
      assert maker.withdrawalHist[0]['status'] == WithdrawInfo.WITHDRAW_COMPLETED
