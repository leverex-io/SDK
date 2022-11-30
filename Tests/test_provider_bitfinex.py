#import pdb; pdb.set_trace()
import unittest
from unittest.mock import patch

from .utils import TestMaker, getOrderBookSnapshot
from Hedger.SimpleHedger import SimpleHedger
from Factories.Dealer.Factory import DealerFactory
from Factories.Definitions import AggregationOrderBook

from Providers.Bitfinex import BitfinexProvider, BFX_DERIVATIVES_WALLET
from Providers.bfxapi.bfxapi.models.wallet import Wallet

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

   async def push_authorize(self):
      await self.ws.callbacks['authenticated']("")

   async def push_wallet_snapshot(self, balance):
      await self.ws.callbacks['wallet_snapshot']([
         Wallet(BFX_DERIVATIVES_WALLET,
            'usdt', balance, 0, balance
         )])

   async def push_position_snapshot(self):
      await self.ws.callbacks['position_snapshot']([
         None, None, [[
            'usdt',     #symbol
            None,       #status
            0,          #amount
            10000,      #base price
            0, 0,       #margin funding stuff
            0, 0,       #pl stuff
            0,          #PRICE_LIQ
            15,         #leverage
            None,       #placeholder
            0,          #position id
            0, 0,       #MTS create, update
            None,       #placeholder
            None,       #TYPE
            None,       #placeholder
            0, 0,       #collateral, min collateral
            None        #meta
      ]]])

   def push_orderbook_snapshot(self, volume):
      orders = getOrderBookSnapshot(volume)
      self.ws.callbacks['order_book_snapshot']({
         'data': orders
      })

   async def update_order_book(self, volume, price):
      await self.ws.callbacks['order_book_update']({
         'data' : [
            price,
            #order_count, ignored by AggregationOrderBook for the most part
            2,
            volume]
      })

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
   async def test_bootstrap_1(self, MockedBfxClientObj):
      #return mocked finex connection object instead of an instance
      #of bfxapi.Client
      mockedConnection = MockedBfxClientClass()
      MockedBfxClientObj.return_value = mockedConnection

      #test hedger making/pulling offers
      maker = TestMaker(1000)
      taker = BitfinexProvider(self.config)
      hedger = SimpleHedger(self.config)
      dealer = DealerFactory(maker, taker, hedger)
      await dealer.run()

      #sanity check on mocked connection
      assert taker.connection is mockedConnection

      #sanity check on ready states
      assert maker.isReady() == True
      assert taker.isReady() == False
      assert dealer.isReady() == False
      assert hedger.isReady() == False
      assert taker._connected == False
      assert taker._balanceInitialized == False

      #get exposure should fail if the provider is not ready
      assert taker.getExposure() == None

      #emit authorize notification
      await mockedConnection.push_authorize()
      assert taker.isReady() == False
      assert dealer.isReady() == False
      assert taker._connected == True

      #get exposure should fail if the provider is not ready
      assert taker.getExposure() == None

      #emit wallet snapshot notification
      await mockedConnection.push_wallet_snapshot(1500)
      assert taker.isReady() == False
      assert dealer.isReady() == False
      assert taker._balanceInitialized == True
      assert taker.balances[BFX_DERIVATIVES_WALLET]['usdt']['total'] == 1500

      #get exposure should fail if the provider is not ready
      assert taker.getExposure() == None

      #emit position notification
      await mockedConnection.push_position_snapshot()
      assert taker.isReady() == True
      assert hedger.isReady() == True
      assert dealer.isReady() == True
      assert taker._positionInitialized == True
      assert taker.getExposure() == 0
      assert taker.getOpenVolume() == None

      #push order book snapshot
      mockedConnection.push_orderbook_snapshot(20)

      #check volume
      volume = taker.getOpenVolume()
      assert round(volume['ask'], 4) == 0.993
      assert round(volume['bid'], 4) == 1.0071

      ## check price offers ##
      '''
      offers should be empty right as push_ordebook_snapshot is not
      async, so it cannot trigger the orderbook update signal
      '''
      assert len(maker.offers) == 0

      #order book update will correctly notify the hedger
      await mockedConnection.update_order_book(-20, 10400)
      assert len(maker.offers) == 1
      offers0 = maker.offers[0]
      assert len(offers0) == 2

      assert round(offers0[0].volume, 4) == 0.993
      assert offers0[0].bid == None
      assert offers0[0].ask == round(10020.83 * 1.01, 2)

      assert offers0[1].volume == 1
      assert offers0[1].bid == round(9979.17  * 0.99, 2)
      assert offers0[1].ask == None

   @patch('Providers.Bitfinex.Client')
   async def test_bootstrap_2(self, MockedBfxClientObj):
      #return mocked finex connection object instead of an instance
      #of bfxapi.Client
      mockedConnection = MockedBfxClientClass()
      MockedBfxClientObj.return_value = mockedConnection

      #test hedger making/pulling offers
      maker = TestMaker(1000)
      taker = BitfinexProvider(self.config)
      hedger = SimpleHedger(self.config)
      dealer = DealerFactory(maker, taker, hedger)
      await dealer.run()

      #sanity check on mocked connection
      assert taker.connection is mockedConnection

      #sanity check on ready states
      assert maker.isReady() == True
      assert taker.isReady() == False
      assert dealer.isReady() == False
      assert taker._connected == False
      assert taker._balanceInitialized == False

      #get exposure should fail if the provider is not ready
      assert taker.getExposure() == None

      #emit authorize notification
      await mockedConnection.push_authorize()
      assert taker.isReady() == False
      assert dealer.isReady() == False
      assert taker._connected == True

      #get exposure should fail if the provider is not ready
      assert taker.getExposure() == None

      #emit position notification
      await mockedConnection.push_position_snapshot()
      assert taker.isReady() == False
      assert dealer.isReady() == False
      assert taker._positionInitialized == True

      #get exposure should fail if the provider is not ready
      assert taker.getExposure() == None

      #emit wallet snapshot notification
      await mockedConnection.push_wallet_snapshot(1500)
      assert taker.isReady() == True
      assert taker._balanceInitialized == True
      assert taker.getExposure() == 0
      assert dealer.isReady() == True
      assert taker.balances[BFX_DERIVATIVES_WALLET]['usdt']['total'] == 1500
      assert taker.getOpenVolume() == None

      #push order book snapshot
      mockedConnection.push_orderbook_snapshot(20)

      #check volume
      volume = taker.getOpenVolume()
      assert round(volume['ask'], 4) == 0.993
      assert round(volume['bid'], 4) == 1.0071

      ## check price offers ##
      '''
      offers should be empty right as push_ordebook_snapshot is not
      async, so it cannot trigger the orderbook update signal
      '''
      assert len(maker.offers) == 0

      #order book update will correctly notify the hedger
      await mockedConnection.update_order_book(20, 9600)
      assert len(maker.offers) == 1
      offers0 = maker.offers[0]
      assert len(offers0) == 2

      assert round(offers0[0].volume, 4) == 0.993
      assert offers0[0].bid == None
      assert offers0[0].ask == round(10020.83 * 1.01, 2)

      assert offers0[1].volume == 1
      assert offers0[1].bid == round(9979.17  * 0.99, 2)
      assert offers0[1].ask == None
