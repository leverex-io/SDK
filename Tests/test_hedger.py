#import pdb; pdb.set_trace()
import unittest

from .utils import TestTaker, TestMaker, price
from Factories.Definitions import Order
from Hedger.SimpleHedger import SimpleHedger
from Factories.Dealer.Factory import DealerFactory

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
      assert offers0[0].volume == 1
      assert offers0[0].bid == round(9981.25  * 0.99, 2)
      assert offers0[0].ask == round(10018.75 * 1.01, 2)

      #balance event
      await maker.updateBalance(500)
      assert len(maker.offers) == 2

      offers1 = maker.offers[1]
      assert len(offers1) == 1
      assert offers1[0].volume == 0.5
      assert offers1[0].bid == round(9989.58  * 0.99, 2)
      assert offers1[0].ask == round(10010.42 * 1.01, 2)

      #order book event
      await taker.populateOrderBook(6)
      assert len(maker.offers) == 3

      offers2 = maker.offers[2]
      assert len(offers2) == 1
      assert offers2[0].volume == 0.5
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
      assert offers0[0].volume == 0.5
      assert offers0[0].bid == round(9993.75  * 0.99, 2)
      assert offers0[0].ask == round(10006.25 * 1.01, 2)

      #new order event
      newOrder = Order(id=1, timestamp=0, quantity=0.1, price=10100)
      await maker.newOrder(newOrder)
      assert len(maker.offers) == 2

      #check exposure
      assert maker.getExposure() == 0.1
      assert taker.getExposure() == -0.1

      #check volumes
      makerVolume = maker.getOpenVolume()
      assert makerVolume['ask'] == 0.6
      assert makerVolume['bid'] == 0.4

      takerVolume = taker.getOpenVolume()
      assert takerVolume['ask'] == 0.9
      assert takerVolume['bid'] == 1.1

      #check offers
      offers1 = maker.offers[1]
      assert len(offers1) == 2

      assert offers1[0].volume == 0.6
      assert offers1[0].bid == None
      assert offers1[0].ask == round(10011.25 * 1.01, 2)

      assert offers1[1].volume == 0.4
      assert offers1[1].bid == round(9993.75  * 0.99, 2)
      assert offers1[1].ask == None

   #exposure tests set various exposure on the maker and the taker,
   #then start the dealer and check the exposures are in sync
   async def test_exposure_sync_maker(self):
      #setup taker and maker
      taker = TestTaker(startBalance=1500)

      startOrders = []
      startOrders.append(Order(id=1, timestamp=0, quantity=0.1, price=10100))
      startOrders.append(Order(id=2, timestamp=0, quantity=0.2, price=10150))
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
      newOrder = Order(id=3, timestamp=0, quantity=-0.1, price=9900)
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
      newOrder = Order(id=3, timestamp=0, quantity=0.1, price=9900)
      await maker.newOrder(newOrder)

      assert maker.balance == 1000
      assert taker.balance == 1500
      assert maker.getExposure() == 0.1
      assert taker.getExposure() == -0.1

   async def test_exposure_sync_both(self):
      #setup taker and maker
      taker = TestTaker(startBalance=1500, startExposure=0.5)

      startOrders = []
      startOrders.append(Order(id=1, timestamp=0, quantity=0.3, price=10100))
      startOrders.append(Order(id=2, timestamp=0, quantity=0.1, price=10150))
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

      #check taker exposure is zero'd out since maker has none
      assert maker.balance == 1000
      assert taker.balance == 1500
      assert maker.getExposure() == 0.4
      assert taker.getExposure() == -0.4

      #add another order
      newOrder = Order(id=3, timestamp=0, quantity=-0.1, price=9900)
      await maker.newOrder(newOrder)

      assert maker.balance == 1000
      assert taker.balance == 1500
      assert maker.getExposure() == 0.3
      assert taker.getExposure() == -0.3



#TODO: cover bitfinex provider

################################################################################
if __name__ == '__main__':
   unittest.main()