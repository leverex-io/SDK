import unittest
from unittest.mock import patch

from .utils import TestMaker
from Hedger.SimpleHedger import SimpleHedger
from Factories.Dealer.Factory import DealerFactory
from Providers.Bitfinex import BitfinexProvider
from Factories.Definitions import AggregationOrderBook

################################################################################
##
#### Bitfinex Provider Tests
##
################################################################################
class FakeBfxWsInterface(object):
   def __init__(self):
      self.callbacks = {}
      self.order_books = {}

   def on(self, name, callback):
      self.callbacks[name] = callback

   async def get_task_executable(self):
      pass

   async def subscribe(self, name, product, len, prec):
      self.order_books[name] = AggregationOrderBook()

########
class MockedBfxClientClass(object):
   def __init__(self):
      self.ws = FakeBfxWsInterface()

   async def authorize(self):
      await self.ws.callbacks['authenticated']("")

########
class TestBitfinexProvider(unittest.IsolatedAsyncioTestCase):
   config = {}
   config['bitfinex'] = {
      'api_key' : 'the_key',
      'api_secret' : 'the_secret',
      'futures_hedging_product' : 'usdt',
      'orderbook_product' : 'usdt',
      'derivatives_currency' : 'usdt',
      'min_leverage' : 10,
      'leverage' : 15,
      'max_leverage' : 20
   }
   config['hedging_settings'] = {
      'max_offer_volume' : 5,
      'price_ratio' : 0.01
   }

   @patch('Providers.Bitfinex.Client')
   async def test_bootstrap(self, MockedBfxClientObj):
      #return mocked finex connection object instead of an instance
      #of bfxapi.Client
      mockedConnection = MockedBfxClientClass()
      MockedBfxClientObj.return_value = mockedConnection

      #test hedger making/pulling offers
      maker = TestMaker()
      taker = BitfinexProvider(self.config)
      hedger = SimpleHedger(self.config)
      dealer = DealerFactory(maker, taker, hedger)
      await dealer.run()

      #sanity check on mocked connection
      assert taker.connection is mockedConnection
      assert maker.isReady() == True
      assert taker.isReady() == False
      assert dealer.isReady() == False
      assert taker._connected == False

      #emit authorize notification
      await mockedConnection.authorize()
      assert taker.isReady() == False
      assert dealer.isReady() == False
      assert taker._connected == True
