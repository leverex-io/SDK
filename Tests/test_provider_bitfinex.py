#import pdb; pdb.set_trace()
import unittest
from unittest.mock import patch

from .utils import TestMaker, getOrderBookSnapshot
from Hedger.SimpleHedger import SimpleHedger
from Factories.Dealer.Factory import DealerFactory
from Factories.Definitions import AggregationOrderBook, Order, \
   SIDE_BUY, SIDE_SELL

from Providers.Bitfinex import BitfinexProvider, BFX_DERIVATIVES_WALLET
from Providers.bfxapi.bfxapi.models.wallet import Wallet

AMOUNT = 2
PRICE = 3
LIQ_PRICE = 8
LEVERAGE = 9
COLLATERAL = 17

################################################################################
##
#### Bitfinex Provider Tests
##
################################################################################
class FakeBfxWsInterface(object):
   def __init__(self):
      self.callbacks = {}
      self.order_books = {}
      self.tracked_exposure = 0
      self.positions = {}

   def on(self, name, callback):
      self.callbacks[name] = callback

   async def get_task_executable(self):
      pass

   async def subscribe(self, name, product, len, prec):
      self.order_books[name] = AggregationOrderBook()

   @staticmethod
   def getCollateral(amount, price, leverage):
      #we assume leverage is collateral ratio
      swing = price / leverage
      return abs(swing * amount)

   def setLiquidationPrice(self, symbol):
      amount = self.positions[symbol][AMOUNT]
      if amount == 0:
         self.positions[symbol][LIQ_PRICE] = None
         return

      price = self.positions[symbol][PRICE]
      collateral = self.positions[symbol][COLLATERAL]

      priceBuffer = abs(collateral / amount)
      if amount > 0:
         priceBuffer *= -1
      self.positions[symbol][LIQ_PRICE] = price + priceBuffer

   async def push_position_snapshot(self, symbol, amount, leverage):
      price = 10000
      collateral = self.getCollateral(amount, price, leverage)
      self.positions[symbol] = [
         symbol,                 #symbol
         None,                   #status
         amount,                 #amount
         price,                  #base price
         0, 0,                   #margin funding stuff
         0, 0,                   #pl stuff
         None,                   #PRICE_LIQ
         leverage,               #leverage
         None,                   #placeholder
         0,                      #position id
         0, 0,                   #MTS create, update
         None,                   #placeholder
         'MARKET',               #TYPE
         None,                   #placeholder
         collateral, 100,        #collateral, min collateral
         None                    #meta
      ]
      self.setLiquidationPrice(symbol)

      await self.callbacks['position_snapshot']([
         None, None, [self.positions[symbol]]])

   async def submit_order(self, symbol, leverage, price, amount, market_type):
      price = self.positions[symbol][PRICE]
      exposure = self.positions[symbol][AMOUNT] + amount
      collateral = self.getCollateral(exposure, price, leverage)
      await self.update_offer(symbol, amount, collateral)

   async def update_offer(self, symbol, amount, collateral):
      self.positions[symbol][AMOUNT]    += amount
      self.positions[symbol][COLLATERAL] = collateral
      self.setLiquidationPrice(symbol)

      await self.callbacks['position_update']([
         None, None, self.positions[symbol]])

   async def subscribe_derivative_status(self, symbol):
      pass

################################################################################
class FakeBfxRestInterface(object):
   def __init__(self, listener):
      self.listener = listener

   async def set_derivative_collateral(self, symbol, collateral):
      await self.listener.ws.update_offer(symbol, 0, collateral)

########
class MockedBfxClientClass(object):
   def __init__(self):
      self.ws = FakeBfxWsInterface()
      self.rest = FakeBfxRestInterface(self)
      self.symbol = 'usdt'

   async def push_authorize(self):
      await self.ws.callbacks['authenticated']("")

   async def push_wallet_snapshot(self, balance):
      await self.ws.callbacks['wallet_snapshot']([
         Wallet(BFX_DERIVATIVES_WALLET,
            self.symbol, balance, 0, balance
         )])

   async def push_position_snapshot(self, amount, leverage):
      await self.ws.push_position_snapshot(self.symbol, amount, leverage)

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

   def getPositionExposure(self):
      return round(self.ws.positions[self.symbol][AMOUNT], 8)

   def getPositionLiquidationPrice(self):
      return round(self.ws.positions[self.symbol][LIQ_PRICE], 2)

########
class TestBitfinexProvider(unittest.IsolatedAsyncioTestCase):
   config = {}
   config['bitfinex'] = {
      'api_key' : 'the_key',
      'api_secret' : 'the_secret',
      'futures_hedging_product' : 'usdt',
      'orderbook_product' : 'usdt',
      'derivatives_currency' : 'usdt',
      'collateral_pct' : 15,
      'max_collateral_deviation' : 2
   }
   config['hedging_settings'] = {
      'max_offer_volume' : 5,
      'price_ratio' : 0.01,
      'offer_refresh_delay_ms' : 0
   }

   @patch('Providers.Bitfinex.Client')
   async def test_bootstrap_1(self, MockedBfxClientObj):
      #return mocked finex connection object instead of an instance
      #of bfxapi.Client
      mockedConnection = MockedBfxClientClass()
      MockedBfxClientObj.return_value = mockedConnection

      #setup dealer
      maker = TestMaker(1000)
      taker = BitfinexProvider(self.config)
      hedger = SimpleHedger(self.config)
      dealer = DealerFactory(maker, taker, hedger)
      await dealer.run()

      #sanity check on mocked connection
      assert taker.connection is mockedConnection
      assert taker.leverage == 10

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
      await mockedConnection.push_position_snapshot(0, taker.leverage)
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
      offers should be empty as push_ordebook_snapshot is not
      async, so it cannot trigger the orderbook update notification
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

      #setup dealer
      maker = TestMaker(1000)
      taker = BitfinexProvider(self.config)
      hedger = SimpleHedger(self.config)
      dealer = DealerFactory(maker, taker, hedger)
      await dealer.run()

      #sanity check on mocked connection
      assert taker.connection is mockedConnection
      assert taker.leverage == 10

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

      #emit position notification
      await mockedConnection.push_position_snapshot(0, taker.leverage)
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
      offers should be empty as push_ordebook_snapshot is not
      async, so it cannot trigger the orderbook update notification
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

   @patch('Providers.Bitfinex.Client')
   async def test_bootstrap_with_exposure(self, MockedBfxClientObj):
      #return mocked finex connection object instead of an instance
      #of bfxapi.Client
      mockedConnection = MockedBfxClientClass()
      MockedBfxClientObj.return_value = mockedConnection

      #setup dealer
      maker = TestMaker(1000, [Order(1, 1, -0.2, 10000, SIDE_SELL)])
      taker = BitfinexProvider(self.config)
      hedger = SimpleHedger(self.config)
      dealer = DealerFactory(maker, taker, hedger)
      await dealer.run()

      #sanity check on mocked connection
      assert taker.connection is mockedConnection
      assert taker.leverage == 10

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

      #emit position notification
      await mockedConnection.push_position_snapshot(1, taker.leverage)
      assert taker.isReady() == False
      assert dealer.isReady() == False
      assert taker._positionInitialized == True
      assert mockedConnection.getPositionExposure() == 1

      #get exposure should fail if the provider is not ready
      assert taker.getExposure() == None

      #emit wallet snapshot notification
      await mockedConnection.push_wallet_snapshot(1500)
      assert taker.isReady() == True
      assert taker._balanceInitialized == True
      assert round(taker.getExposure(), 8) == 0.2
      assert dealer.isReady() == True
      assert taker.balances[BFX_DERIVATIVES_WALLET]['usdt']['total'] == 1500
      assert mockedConnection.getPositionExposure() == 0.2

   @patch('Providers.Bitfinex.Client')
   async def test_exposure_sync(self, MockedBfxClientObj):
      #return mocked finex connection object instead of an instance
      #of bfxapi.Client
      mockedConnection = MockedBfxClientClass()
      MockedBfxClientObj.return_value = mockedConnection

      #setup dealer
      maker = TestMaker(1000)
      taker = BitfinexProvider(self.config)
      hedger = SimpleHedger(self.config)
      dealer = DealerFactory(maker, taker, hedger)
      await dealer.run()

      #sanity check on mocked connection
      assert taker.connection is mockedConnection
      assert taker.leverage == 10

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

      #emit position notification
      await mockedConnection.push_position_snapshot(0, taker.leverage)
      assert taker.isReady() == False
      assert dealer.isReady() == False
      assert taker._positionInitialized == True
      assert mockedConnection.ws.tracked_exposure == 0

      #get exposure should fail if the provider is not ready
      assert taker.getExposure() == None

      #emit wallet snapshot notification
      await mockedConnection.push_wallet_snapshot(1500)
      assert taker.isReady() == True
      assert taker._balanceInitialized == True
      assert round(taker.getExposure(), 8) == 0
      assert dealer.isReady() == True
      assert taker.balances[BFX_DERIVATIVES_WALLET]['usdt']['total'] == 1500

      #notify maker of new order, taker exposure should be updated accordingly
      await maker.newOrder(Order(1, 1, 1, 10200, SIDE_BUY))
      assert round(maker.getExposure(), 8) == 1
      assert round(taker.getExposure(), 8) == -1

      #another one
      await maker.newOrder(Order(2, 2, -0.6, 9900, SIDE_SELL))
      assert round(maker.getExposure(), 8) == 0.4
      assert round(taker.getExposure(), 8) == -0.4

   @patch('Providers.Bitfinex.Client')
   async def test_adjust_collateral(self, MockedBfxClientObj):
      #return mocked finex connection object instead of an instance
      #of bfxapi.Client
      mockedConnection = MockedBfxClientClass()
      MockedBfxClientObj.return_value = mockedConnection

      #setup dealer
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

      #emit position notification
      await mockedConnection.push_position_snapshot(0, taker.leverage)
      assert taker.isReady() == False
      assert dealer.isReady() == False
      assert taker._positionInitialized == True
      assert mockedConnection.ws.tracked_exposure == 0

      #get exposure should fail if the provider is not ready
      assert taker.getExposure() == None

      #emit wallet snapshot notification
      await mockedConnection.push_wallet_snapshot(1500)
      assert taker.isReady() == True
      assert taker._balanceInitialized == True
      assert round(taker.getExposure(), 8) == 0
      assert dealer.isReady() == True
      assert taker.balances[BFX_DERIVATIVES_WALLET]['usdt']['total'] == 1500

      #notify maker of new order, taker exposure should be updated accordingly
      await maker.newOrder(Order(1, 1, 1, 10200, SIDE_BUY))
      assert round(maker.getExposure(), 8) == 1
      assert round(taker.getExposure(), 8) == -1

      #check collateral and liquidation price
      assert mockedConnection.getPositionLiquidationPrice() == 11500

      #another one
      await maker.newOrder(Order(2, 2, -0.6, 9900, SIDE_SELL))
      assert round(maker.getExposure(), 8) == 0.4
      assert round(taker.getExposure(), 8) == -0.4

      #check collateral and liquidation price
      assert mockedConnection.getPositionLiquidationPrice() == 11500

      #set maker's open price way above the position's base price
      await maker.setOpenPrice(12000)
      assert mockedConnection.getPositionLiquidationPrice() == 13800

      #set maker's open price way below the position's base price
      await maker.setOpenPrice(8000)
      assert mockedConnection.getPositionLiquidationPrice() == 10250
