import pdb; pdb.set_trace()
import unittest
from unittest.mock import patch

from .utils import TestMaker, getOrderBookSnapshot
from Hedger.SimpleHedger import SimpleHedger
from Factories.Dealer.Factory import DealerFactory
from Factories.Definitions import AggregationOrderBook, Order, \
   SIDE_BUY, SIDE_SELL

from Providers.Bitfinex import BitfinexProvider, BfxAccounts, \
   productToCcy
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
def methodToCcy(method):
   if method == 'TETHERUSL':
      return 'USDT'
   return None

####
class FakeBfxWsInterface(object):
   def __init__(self):
      self.callbacks = {}
      self.order_books = {}
      self.tracked_exposure = 0
      self.positions = {}
      self.balance = {}

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

      #push wallet balance notification to update free cash
      balance = 0
      ccy = productToCcy(symbol)
      if BfxAccounts.DERIVATIVES in self.balance:
         balance = self.balance[BfxAccounts.DERIVATIVES][ccy]
      await self.push_wallet_update(symbol, balance, BfxAccounts.DERIVATIVES)

   async def subscribe_derivative_status(self, symbol):
      pass

   def getFreeBalance(self, symbol, wallet):
      ccy = productToCcy(symbol)
      val = 0
      if symbol in self.positions:
         val = self.positions[symbol][COLLATERAL]
      return self.balance[wallet][ccy] - val

   async def push_wallet_snapshot(self, symbol, balance, acc):
      #track wallets internally
      ccy = productToCcy(symbol)
      if acc not in self.balance:
         self.balance[acc] = {}
      self.balance[acc][ccy] = balance

      #push notification to finex provider
      await self.callbacks['wallet_snapshot']([
         Wallet(acc, ccy, balance,
            0, self.getFreeBalance(symbol, acc)
      )])

   async def push_wallet_update(self, symbol, balance, acc):
      #track wallets internally
      ccy = productToCcy(symbol)
      if acc not in self.balance:
         self.balance[acc] = {}
      self.balance[acc][ccy] = balance

      #push notification to finex provider
      await self.callbacks['wallet_update'](
         Wallet(acc, ccy, balance,
            0, self.getFreeBalance(symbol, acc)
      ))

####
class FakeBfxRestInterface(object):
   def __init__(self, listener):
      self.listener = listener
      self.withdrawals = []
      self.cash_movements = []

   async def set_derivative_collateral(self, symbol, collateral):
      await self.listener.ws.update_offer(symbol, 0, collateral)

   async def get_wallet_deposit_address(self, wallet, method):
      #
      class AddrObj():
         def __init__(self, addr):
            self.address = addr

      #
      class AddrNotif():
         def __init__(self, addr):
            self.notify_info = AddrObj(addr)

      return AddrNotif("efgh")

   async def submit_wallet_withdraw(self, wallet, method, amount, address):
      self.withdrawals.append({
         'wallet': wallet,
         'method': method,
         'amount': amount,
         'address': address
      })

   async def submit_wallet_transfer(self, from_wallet, to_wallet,
      from_currency, to_currency, amount):
      self.cash_movements.append({
         'from': from_wallet,
         'to': to_wallet,
         'ccy_from': from_currency,
         'ccy_to': to_currency,
         'amount': amount
      })

   async def get_movement_history(self, ccy, start="", end="", limit=25):
      return []

####
class MockedBfxClientClass(object):
   def __init__(self):
      self.ws = FakeBfxWsInterface()
      self.rest = FakeBfxRestInterface(self)
      self.product = 'BTCF0:USDTF0'
      self.ccy = 'USDTF0'

   async def push_authorize(self):
      await self.ws.callbacks['authenticated']("")

   async def push_wallet_snapshot(self, balance):
      await self.ws.push_wallet_snapshot(self.product, balance, BfxAccounts.DERIVATIVES)

   async def push_wallet_update(self, balance, wallet=BfxAccounts.DERIVATIVES):
      await self.ws.push_wallet_update(self.ccy, balance, wallet)

   async def push_position_snapshot(self, amount, leverage):
      await self.ws.push_position_snapshot(self.product, amount, leverage)

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
      return round(self.ws.positions[self.product][AMOUNT], 8)

   def getPositionLiquidationPrice(self):
      return round(self.ws.positions[self.product][LIQ_PRICE], 2)

   async def completeCashMovement(self):
      #grab pending movements from rest interface
      movements = self.rest.cash_movements
      self.rest.cash_movements = []

      #aggragate effect
      aggregate = {}
      for mvmt in movements:
         fromAcc = mvmt['from']
         toAcc   = mvmt['to']

         if not fromAcc in aggregate:
            aggregate[fromAcc] = {}
         if not toAcc in aggregate:
            aggregate[toAcc] = {}

         if not mvmt['ccy_from'] in aggregate[fromAcc]:
            aggregate[fromAcc][mvmt['ccy_from']] = 0
         if not mvmt['ccy_to'] in aggregate[toAcc]:
            aggregate[toAcc][mvmt['ccy_to']] = 0

         aggregate[fromAcc][mvmt['ccy_from']] -= mvmt['amount']
         aggregate[toAcc][mvmt['ccy_to']]     += mvmt['amount']

      #apply to balance
      balances = self.ws.balance
      for acc in aggregate:
         initialAcc = {}
         if acc in balances:
            initialAcc = balances[acc]
         for ccy in aggregate[acc]:
            initialBal = 0
            if ccy in initialAcc:
               initialBal = initialAcc[ccy]
            bal = aggregate[acc][ccy] + initialBal
            await self.ws.push_wallet_update(ccy, bal, acc)

   async def completeWithdrawals(self):
      withdrawals = self.rest.withdrawals
      self.rest.withdrawals = []

      for wtd in withdrawals:
         ccy = methodToCcy(wtd['method'])
         bal = self.ws.balance[wtd['wallet']][ccy]
         await self.ws.push_wallet_update(
            ccy,
            bal - wtd['amount'],
            wtd['wallet']
         )

########
class TestBitfinexProvider(unittest.IsolatedAsyncioTestCase):
   config = {}
   config['bitfinex'] = {
      'api_key' : 'the_key',
      'api_secret' : 'the_secret',
      'product' : 'BTCF0:USDTF0',
      'collateral_pct' : 15,
      'max_collateral_deviation' : 2,
      'deposit_method' : 'TETHERUSL'
   }
   config['hedger'] = {
      'max_offer_volume' : 5,
      'price_ratio' : 0.01,
      'offer_refresh_delay_ms' : 0,
      'min_size' : 0.00006
   }
   config['rebalance'] = {
      'enable' : True,
      'threshold_pct' : 0.1,
      'min_amount' : 10
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
      assert taker.balances[BfxAccounts.DERIVATIVES]['USDTF0']['total'] == 1500

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

      assert offers0[0].volume == 1
      assert offers0[0].bid == None
      assert offers0[0].ask == round(10020.83 * 1.01, 2)

      assert round(offers0[1].volume, 4) == 0.993
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
      assert taker.balances[BfxAccounts.DERIVATIVES]['USDTF0']['total'] == 1500
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

      assert offers0[0].volume == 1
      assert offers0[0].bid == None
      assert offers0[0].ask == round(10020.83 * 1.01, 2)

      assert round(offers0[1].volume, 4) == 0.993
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
      assert taker.balances[BfxAccounts.DERIVATIVES]['USDTF0']['total'] == 1500
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
      assert taker.balances[BfxAccounts.DERIVATIVES]['USDTF0']['total'] == 1500

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
      assert taker.balances[BfxAccounts.DERIVATIVES]['USDTF0']['total'] == 1500

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

   #cover open volume asymetry
   @patch('Providers.Bitfinex.Client')
   async def test_open_volume(self, MockedBfxClientObj):
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
      await mockedConnection.update_order_book(-20, 10400)
      await mockedConnection.update_order_book(20, 9600)

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
      assert hedger.isReady() == True
      assert dealer.isReady() == True
      assert taker.balances[BfxAccounts.DERIVATIVES]['USDTF0']['total'] == 1500
      assert maker.balance == 1000

      #check open volume
      vol = taker.getOpenVolume()
      assert vol['ask'] == 1500 / (0.15 * 10400)
      assert vol['bid'] == 1500 / (0.15 * 9600)

      ## push an order ##
      await maker.newOrder(Order(1, 1, 0.3, 10000, SIDE_BUY))

      #check exposure
      assert maker.getExposure() == 0.3
      assert taker.getExposure() == -0.3

      #check open volume, should reflect effect of exposure
      vol = taker.getOpenVolume()
      assert vol['ask'] == 1050 / (0.15 * 10400)
      assert vol['bid'] == 1950 / (0.15 * 9600)

      ## push order on opposite side ##
      await maker.newOrder(Order(2, 1, -0.5, 10000, SIDE_SELL))

      #check exposure
      assert maker.getExposure() == -0.2
      assert taker.getExposure() == 0.2

      #check open volume, should reflect effect of exposure
      vol = taker.getOpenVolume()
      assert vol['ask'] == 1800 / (0.15 * 10400)
      assert vol['bid'] == 1200 / (0.15 * 9600)

      ## go flat at a higher price ##
      await maker.newOrder(Order(3, 1, 0.2, 10100, SIDE_BUY))

      #check exposure
      assert maker.getExposure() == 0
      assert taker.getExposure() == 0

      vol = taker.getOpenVolume()
      assert vol['ask'] == 1500 / (0.15 * 10400)
      assert vol['bid'] == 1500 / (0.15 * 9600)

   @patch('Providers.Bitfinex.Client')
   async def test_rebalance_withdrawals(self, MockedBfxClientObj):
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
      await mockedConnection.update_order_book(-20, 10400)

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
      assert hedger.isReady() == True
      assert dealer.isReady() == True
      assert taker.balances[BfxAccounts.DERIVATIVES]['USDTF0']['total'] == 1500
      assert maker.balance == 1000

      #check hedger can rebalance
      assert hedger.canRebalance() == True
      assert hedger.needsRebalance() == False
      assert hedger.rebalMan.canAssess() == True
      assert len(mockedConnection.rest.withdrawals) == 0

      #set taker balance to trigger rebalance
      await mockedConnection.push_wallet_update(4000)
      assert taker.balances[BfxAccounts.DERIVATIVES]['USDTF0']['total'] == 4000
      assert maker.balance == 1000
      assert hedger.canRebalance() == True
      assert hedger.needsRebalance() == True
      assert hedger.rebalMan.canAssess() == False

      cashMetrics = taker.getCashMetrics()
      assert cashMetrics['total'] == 4000
      assert cashMetrics['pending'] == 0

      target = hedger.rebalMan.target
      assert target.maker.target == 2000
      assert target.taker.target == 3000
      assert target.maker.toWithdraw['amount'] == 0
      assert target.taker.toWithdraw['amount'] == 1000
      assert target.taker.toWithdraw['status'] == 5
      assert target.state == 4

      assert len(mockedConnection.rest.withdrawals) == 0
      assert len(mockedConnection.rest.cash_movements) == 1
      assert mockedConnection.rest.cash_movements[0]['amount'] == 1000

      #add exposure, shouldnt affect cash metrics
      await maker.newOrder(Order(1, 1, 0.3, 10000, SIDE_BUY))
      assert maker.getExposure() == 0.3
      assert taker.getExposure() == -0.3
      assert target == hedger.rebalMan.target

      cashMetrics = taker.getCashMetrics()
      assert cashMetrics['total'] == 4000
      assert cashMetrics['pending'] == 0

      #complete cash movement for new withdrawal
      await mockedConnection.completeCashMovement()

      cashMetrics = taker.getCashMetrics()
      assert cashMetrics['total'] == 3000
      assert cashMetrics['pending'] == 1000

      target = hedger.rebalMan.target
      assert target.maker.target == 2000
      assert target.taker.target == 3000
      assert target.maker.toWithdraw['amount'] == 0
      assert target.taker.toWithdraw['amount'] == 1000
      assert target.taker.toWithdraw['status'] == 5
      assert target.state == 4

      assert len(mockedConnection.rest.withdrawals) == 1
      assert len(mockedConnection.rest.cash_movements) == 0
      assert mockedConnection.rest.withdrawals[0]['amount'] == 1000

      #apply effect of withdrawal on taker's balance
      await mockedConnection.completeWithdrawals()
      assert taker.balances[BfxAccounts.DERIVATIVES]['USDTF0']['total'] == 3000
      assert maker.balance == 1000
      assert hedger.canRebalance() == True

      assert target != hedger.rebalMan.target
      assert target.state == 5
      assert hedger.rebalMan.canAssess() == True

   @patch('Providers.Bitfinex.Client')
   async def test_rebalance_withdrawals_with_cancel(self, MockedBfxClientObj):
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
      await mockedConnection.update_order_book(-20, 10400)

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
      assert hedger.isReady() == True
      assert dealer.isReady() == True
      assert taker.balances[BfxAccounts.DERIVATIVES]['USDTF0']['total'] == 1500
      assert maker.balance == 1000

      #check hedger can rebalance
      assert hedger.canRebalance() == True
      assert hedger.needsRebalance() == False
      assert hedger.rebalMan.canAssess() == True
      assert len(mockedConnection.rest.withdrawals) == 0

      #send cash to exchange account, will be seen as pending
      #withdrawal, this should trigger a rebalance
      await mockedConnection.push_wallet_update(1000, BfxAccounts.EXCHANGE)

      cashMetrics = taker.getCashMetrics()
      assert cashMetrics['total'] == 1500
      assert cashMetrics['pending'] == 1000

      assert hedger.canRebalance() == True
      assert hedger.needsRebalance() == True
      assert hedger.rebalMan.canAssess() == False

      target = hedger.rebalMan.target
      assert target.taker.target == 2100
      assert target.maker.target == 1400
      assert target.state == 3
      assert target.taker.cancelPending['status'] == 2
      assert target.taker.toWithdraw['amount'] == 400
      assert target.taker.toWithdraw['status'] == 4
      assert len(mockedConnection.rest.withdrawals) == 0
      assert len(mockedConnection.rest.cash_movements) == 1
      assert mockedConnection.rest.cash_movements[0]['amount'] == 1000

      #complete cash movement for withdrawal cancel
      await mockedConnection.completeCashMovement()

      cashMetrics = taker.getCashMetrics()
      assert cashMetrics['total'] == 2500
      assert cashMetrics['pending'] == 0

      assert hedger.canRebalance() == True
      assert hedger.needsRebalance() == True
      assert hedger.rebalMan.canAssess() == False

      target = hedger.rebalMan.target
      assert target.taker.target == 2100
      assert target.maker.target == 1400
      assert target.state == 4
      assert target.taker.cancelPending['status'] == 3
      assert target.taker.toWithdraw['amount'] == 400
      assert target.taker.toWithdraw['status'] == 5
      assert len(mockedConnection.rest.withdrawals) == 0
      assert len(mockedConnection.rest.cash_movements) == 1

      #complete cash movement for new withdrawal
      await mockedConnection.completeCashMovement()
      cashMetrics = taker.getCashMetrics()
      assert cashMetrics['total'] == 2100
      assert cashMetrics['pending'] == 400

      assert hedger.canRebalance() == True
      assert hedger.needsRebalance() == True
      assert hedger.rebalMan.canAssess() == False

      target = hedger.rebalMan.target
      assert target.taker.target == 2100
      assert target.maker.target == 1400
      assert target.state == 4
      assert target.taker.toWithdraw['amount'] == 400
      assert target.taker.toWithdraw['status'] == 5
      assert len(mockedConnection.rest.withdrawals) == 1
      assert len(mockedConnection.rest.cash_movements) == 0
      assert mockedConnection.rest.withdrawals[0]['amount'] == 400

      #complete withdrawal
      await mockedConnection.completeWithdrawals()
      cashMetrics = taker.getCashMetrics()
      assert cashMetrics['total'] == 2100
      assert cashMetrics['pending'] == 0

      assert hedger.canRebalance() == True
      assert hedger.needsRebalance() == False
      assert hedger.rebalMan.canAssess() == True
      assert target.state == 5
      assert target.taker.toWithdraw['status'] == 6
      assert target != hedger.rebalMan.target

   @patch('Providers.Bitfinex.Client')
   async def test_cash_conversion(self, MockedBfxClientObj):
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
      assert taker.balances[BfxAccounts.DERIVATIVES]['USDTF0']['total'] == 1500
      assert hedger.canRebalance() == True

      #deposit usdt to exchange account
      await mockedConnection.ws.push_wallet_update('USDT', 500, BfxAccounts.EXCHANGE)
      cash = taker.getCashMetrics()
      assert cash['total'] == 1500
      assert cash['pending'] == 500

      #complete cash transfer
      await mockedConnection.completeCashMovement()
      cash = taker.getCashMetrics()
      assert cash['total'] == 2000
      assert cash['pending'] == 0
