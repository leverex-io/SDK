import time
from datetime import datetime

### order enums ###
ORDER_ACTION_CREATED = 1
ORDER_ACTION_UPDATED = 2

ORDER_STATUS_PENDING = 1
ORDER_STATUS_FILLED  = 2

ORDER_TYPE_TRADE_POSITION                 = 0
ORDER_TYPE_NORMAL_ROLLOVER_POSITION       = 1
ORDER_TYPE_LIQUIDATED_ROLLOVER_POSITION   = 2
ORDER_TYPE_DEFAULTED_ROLLOVER_POSITION    = 3

SIDE_BUY = 1
SIDE_SELL = 2

### balance dict keys ###
kBalanceSectionKey = 'balances'
kBalanceKey = 'balance'
kMaxBuyKey = 'max_amount_buy'
kMaxSellKey = 'max_amount_sell'
kCurrencyKey = 'currency'
kQuantityKey = 'qty'

### exceptions ###
class OfferException(Exception):
   pass

class LeverexException(Exception):
   pass

### session info ###
class SessionOpenInfo():
   def __init__(self, data):
      self.product_type = data['product_type']
      self.cut_off_at = datetime.fromtimestamp(data['cut_off_at'])
      self.last_cut_off_price = float(data['last_cut_off_price'])
      self.session_id = int(data['session_id'])
      self.previous_session_id = data['previous_session_id']
      self._healthy = data['healthy']

####
class SessionCloseInfo():
   def __init__(self, data):
      self.product_type = data['product_type']
      self.session_id = data['session_id']
      self._healthy = data['healthy']

####
class SessionInfo():
   def __init__(self, sessionObject):
      self.open = None
      self.close = None

      if isinstance(sessionObject, SessionOpenInfo):
         self.open = sessionObject
      elif isinstance(sessionObject, SessionCloseInfo):
         self.close = sessionObject

   def isOpen(self):
      if self.close is None and self.open is not None:
         return True
      return False

   def isHealthy(self):
      if self.open != None:
         return self.open._healthy
      elif self.close != None:
         return self.close._healthy
      else:
         raise Exception("invalid session object")

   def getOpenPrice(self):
      if self.open is None:
         return 0
      return self.open.last_cut_off_price

   def getSessionIM(self):
      return self.getOpenPrice() / 10

   def getSessionId(self):
      if self.open != None:
         return self.open.session_id
      elif self.close != None:
         return self.close.session_id
      else:
         raise Exception("invalid session object")


### offers ###
class PriceOffer():
   def __init__(self, volume, ask=None, bid=None, isLast=False):
      if volume:
         self._volume = round(volume, 8)
      else:
         self._volume = None

      if self._volume == 0 or (ask == 0 and bid == 0):
            raise OfferException()

      self._ask = ask
      self._bid = bid
      self._timestamp = time.time_ns() / 1000000 #time in ms
      self._isLast = isLast

   @property
   def volume(self):
      return self._volume

   @property
   def ask(self):
      return self._ask

   @property
   def bid(self):
      return self._bid

   @property
   def isLast(self):
      return self._isLast

   def to_map(self):
      if self._ask is None and self._bid is None:
         return None

      result = {}
      result['volume'] = str(self._volume)
      if self._ask is not None:
         result['ask'] = str(self._ask)
      if self._bid is not None:
         result['bid'] = str(self._bid)
      return result

   def compare(self, offer, delay_ms):
      if offer._timestamp <= self._timestamp + delay_ms:
         #return false if delay is met
         return False

      if self._volume != offer._volume:
         return False
      if self._ask != offer._ask or self._bid != offer._bid:
         return False

      return True

   def __str__(self):
      return f"vol: {self.volume} - ask: {self.ask}, bid: {self.bid}"

   def isValid(self):
      return self._volume != None and self._volume > 0

####
class DealerOffers(object):
   def __init__(self, jsonPacket):
      self.asks = []
      self.bids = []

      if not 'offers' in jsonPacket:
         return

      for offer in jsonPacket['offers']:
         if offer['command'] == 0:
            continue
         if offer['side'] == SIDE_BUY:
            self.bids.append(PriceOffer(
               float(offer['volume']), bid=float(offer['price'])))
         else:
            self.asks.append(PriceOffer(
               float(offer['volume']), ask=float(offer['price'])))

      #sort the offers
      self.bids.sort(key=lambda b : b.bid)
      if len(self.bids) > 0:
         self.bids[-1]._isLast = True

      self.asks.sort(reverse=True, key=lambda a : a.ask)
      if len(self.asks) > 0:
         self.asks[-1]._isLast = True

   def getAsk(self, vol: float):
      if len(self.asks) == 0:
         return PriceOffer(0, None, isLast=True)

      for ask in self.asks:
         if ask.volume >= vol:
            return ask
      return self.asks[-1]

   def getBid(self, vol: float):
      if len(self.bids) == 0:
         return PriceOffer(0, None, isLast=True)

      for bid in self.bids:
         if bid.volume >= vol:
            return bid
      return self.bids[-1]

### orders ###
class Order():
   def __init__(self, id, timestamp, quantity, price, side):
      self._id = id
      self._timestamp = timestamp
      self._quantity = abs(quantity)
      self._price = price
      self._side = side

   @property
   def id(self):
      return self._id

   @property
   def timestamp(self):
      return self._timestamp

   def is_sell(self):
      return self._side == SIDE_SELL

   @property
   def quantity(self):
      return self._quantity

   @property
   def price(self):
      return self._price

####
class LeverexOrder(Order):
   def __init__(self, data):
      super().__init__(data['id'],
         data['timestamp'],
         float(data['quantity']),
         float(data['price']),
         int(data['side'])
      )

      self._status = int(data['status'])
      self._product_type = data['product_type']
      self._trade_pnl = None
      self._reference_exposure = float(data['reference_exposure'])
      self._session_id = int(data['session_id'])
      self._rollover_type = data['rollover_type']
      self._fee = data['fee']
      self._is_taker = data['is_taker']

      self.indexPrice = None
      self.sessionIM = None

   def is_filled(self):
      return self._status == ORDER_STATUS_FILLED

   @property
   def product_type(self):
      return self._product_type

   @property
   def is_taker(self):
      return self._is_taker

   '''
   @property
   def cut_off_price(self):
      return self._cut_off_price

   @property
   def trade_im(self):
      return self._trade_im
   '''

   @property
   def trade_pnl(self):
      return self._trade_pnl

   @property
   def session_id(self):
      return self._session_id

   def is_trade_position(self):
      return self._rollover_type == ORDER_TYPE_TRADE_POSITION

   @property
   def is_rollover_liquidation(self):
      return self._rollover_type == ORDER_TYPE_LIQUIDATED_ROLLOVER_POSITION

   @property
   def is_rollover_default(self):
      return self._rollover_type == ORDER_TYPE_DEFAULTED_ROLLOVER_POSITION

   @property
   def fee(self):
      return self._fee

   @staticmethod
   def tradeTypeStr(tradeType):
      if tradeType == ORDER_TYPE_LIQUIDATED_ROLLOVER_POSITION:
         return "LIQUIDATED"
      elif tradeType == ORDER_TYPE_DEFAULTED_ROLLOVER_POSITION:
         return "DEFAULTED"
      return None

   def __str__(self):
      pl = self.trade_pnl
      if isinstance(pl, float):
         pl = round(pl, 6)

      vol = self.quantity
      if self.is_sell():
         vol *= -1

      text = f"<id: {self.id} -- vol: {vol}, price: {self.price}, pnl: {pl}, fee: {self.fee}"
      tradeType = self.tradeTypeStr(self._rollover_type)
      if tradeType:
         text += " -- ROLL, {}: {}".format(tradeType, \
            abs(self._reference_exposure) - self.quantity)
      elif not self.is_trade_position():
         text += " -- ROLL"
      text += ">"

      return text

   def setSessionIM(self, session):
      if session == None:
         return
      if self.session_id != session.getSessionId():
         return

      self.sessionIM = session.getSessionIM()

   def setIndexPrice(self, price):
      self.indexPrice = price
      self.computePnL()

   def computePnL(self):
      if self.indexPrice == None or self.sessionIM == None:
         self._trade_pnl = None
         return

      #calculate difference between entry and index price
      #cap by max nominal move
      priceDelta = abs(self.indexPrice - self.price)
      priceDelta = min(priceDelta, self.sessionIM)

      #apply operation sign
      if self.price > self.indexPrice:
         priceDelta = -priceDelta

      #apply side sign
      if self.is_sell():
         priceDelta = -priceDelta

      #set pnl
      self._trade_pnl = self.quantity * priceDelta

   def getMargin(self):
      if self.sessionIM == None:
         return None

      return self.sessionIM * self.quantity

   def getValue(self, price):
      if price > self.price + self.sessionIM:
         price = self.price + self.sessionIM
      elif price < self.price - self.sessionIM:
         price = self.price - self.sessionIM

      sign = 1
      if self.is_sell():
         sign = -1
      return self.quantity * (price - self.price) * sign

####
class SessionOrders(object):
   def __init__(self, sessionId):
      self.id = sessionId
      self.orders = {}
      self.netExposure = 0
      self.session = None

   def setSessionObj(self, sessionObj):
      if sessionObj.getSessionId() != self.id:
         return
      self.session = sessionObj
      for order in self.orders:
         self.orders[order].setSessionIM(self.session)

   def setIndexPrice(self, price):
      for order in self.orders:
         self.orders[order].setIndexPrice(price)

   def setOrder(self, order, eventType):
      #set session IM
      if self.session != None:
         order.setSessionIM(self.session)

      self.orders[order.id] = order
      if order.is_filled() and eventType == ORDER_ACTION_UPDATED:
         #filled orders do not affect exposure, return false
         return False

      vol = order.quantity
      if order.is_sell():
         vol *= -1
      self.netExposure += vol

      #return true if setting this order affected net exposure
      return True

   def getNetExposure(self):
      return round(self.netExposure, 8)

   def getCount(self):
      return len(self.orders)

   def __eq__(self, obj):
      if obj == None:
         return False

      return self.id == obj.id and self.orders.keys() == obj.orders.keys()

### max calcs ###
class LeverexOpenVolume(object):
   def __init__(self, provider):
      #open balance
      self.openBalance = 0
      if provider.ccy in provider.balances:
         self.openBalance = provider.balances[provider.ccy]

      self.margin = 0
      if provider.margin_ccy in provider.balances:
         self.margin = provider.balances[provider.margin_ccy]

      #index price
      self.indexPrice = provider.indexPrice

      #session
      self.session = provider.currentSession

      #orders
      self.orders = None
      if self.session.getSessionId() in provider.orderData:
         self.orders = provider.orderData[self.session.getSessionId()].orders

      #remove fee from margin
      totalFee = 0
      for orderId in self.orders:
         order = self.orders[orderId]
         if not (order.is_trade_position() and order.is_taker):
            continue
         totalFee += abs(order.fee)
      self.margin -= totalFee

   def getReleasableExposure(self, askPrice, bidPrice):
      if not self.session.isHealthy() or \
         self.orders is None or \
         len(self.orders) == 0:
         return 0, 0

      #get the sessionIM
      sessionIM = self.session.getSessionIM()

      #get boundaries
      boundaries = set()
      for orderId in self.orders:
         orderPrice = self.orders[orderId].price
         boundaries.add(orderPrice + sessionIM)
         boundaries.add(orderPrice - sessionIM)

      #inject current index price in boundaries
      maxSellPrice = None
      if askPrice != 0:
         maxSellPrice = askPrice + sessionIM
         boundaries.add(maxSellPrice)

      maxBuyPrice = None
      if bidPrice != 0:
         maxBuyPrice = bidPrice - sessionIM
         boundaries.add(maxBuyPrice)

      #order the boundaries
      boundaries = sorted(boundaries)

      #get values at boundaries
      values = {}
      highestValue = 0
      for price in boundaries:
         value = 0
         for orderId in self.orders:
            order = self.orders[orderId]
            value += order.getValue(price)
         values[price] = value
         highestValue = max(highestValue, value)
      valuesList = list(values)

      #sell side: find what's to the right of the max loss
      maxSellLoss = 0
      if maxSellPrice:
         maxSellLoss = highestValue
         for i in range(0, len(values)):
            price = valuesList[i]
            if price >= maxSellPrice:
               maxSellLoss = min(maxSellLoss, values[price])
         maxSellLoss += self.margin

      #buy side: find what's to the left of the max loss
      maxBuyLoss = 0
      if maxBuyPrice:
         maxBuyLoss = highestValue
         for i in range(0, len(values)):
            price = valuesList[i]
            if price > maxBuyPrice:
               break
            maxBuyLoss = min(maxBuyLoss, values[price])
         maxBuyLoss += self.margin

      return round(maxBuyLoss / sessionIM , 8), round(maxSellLoss / sessionIM, 8)

   def get(self, maxVolume, unquoteRatio):
      #get releasable exposure for both sides
      releasableBuy, releaseableSell = self.getReleasableExposure(
         self.indexPrice, self.indexPrice)

      #convert free balance to exposure
      openBal = self.openBalance / self.session.getSessionIM()

      #add releasble exposure
      maxBuy = openBal + releasableBuy
      maxSell = openBal + releaseableSell

      #limit by max volume, apply unquote ratio
      #unquote ratio is the portion of the available exposure
      #that should be kept unencumbured at all times
      sellVol = round(min(maxVolume, maxSell) * (1.0 - unquoteRatio), 8)
      buyVol = round(min(maxVolume, maxBuy) * (1.0 - unquoteRatio), 8)

      return {
         'ask': sellVol,
         'bid': buyVol
      }

### balance ###
def getBalancesFromJson(jsonDict):
   result = {}
   if kBalanceSectionKey in jsonDict:
      for account in jsonDict[kBalanceSectionKey]:
         if not kBalanceKey in account or not kCurrencyKey in account:
            continue
         result[account[kCurrencyKey]] = float(account[kBalanceKey])

   return result

### transfers ###
class WithdrawInfo(object):
   WITHDRAW_FAILED      = 0
   WITHDRAW_ACCEPTED    = 1
   WITHDRAW_PENDING     = 2
   WITHDRAW_BROADCASTED = 3
   WITHDRAW_COMPLETED   = 4
   WITHDRAW_CANCELLED   = 5
   WITHDRAW_BATCHED     = 6

   status_text = {
      WITHDRAW_FAILED : 'failed',
      WITHDRAW_ACCEPTED : 'accepted',
      WITHDRAW_PENDING : 'pending',
      WITHDRAW_BROADCASTED : 'broadcasted',
      WITHDRAW_COMPLETED : 'completed',
      WITHDRAW_CANCELLED : 'cancelled',
      WITHDRAW_BATCHED : 'batched'
   }

   def __init__(self, data):
      self._id = str(data['id'])
      self._status = int(data['status'])
      if 'success' in data:
         if data['success']:
            self._error_message = None
         else:
            self._error_message = data['error_msg']
            return

      self._tx_id = str(data.get('tx_id', ''))
      self._recv_address = str(data['recv_address'])
      self._currency = str(data['currency'])
      self._amount = str(data['amount'])
      self._timestamp = datetime.fromtimestamp(data['timestamp'])
      self._unblinded_link = str(data.get('unblinded_link', ''))
      self._error_message = None

   def __str__(self):
      result = f'<id: {self._id}> amount: {self.amount}, ccy: {self.currency}, status: {self.status}'
      if len(self._tx_id) > 0:
         result += f'tx id: {self._tx_id}. link: {self._unblinded_link}'
      return result

   @property
   def id(self):
      return self._id

   @property
   def status_code(self):
      return self._status

   @property
   def status(self):
      return self.status_text.get(self._status, "Undefined")

   @property
   def error_message(self):
      return self._error_message

   @property
   def recv_address(self):
      return self._recv_address

   @property
   def currency(self):
      return self._currency

   @property
   def amount(self):
      return self._amount

   @property
   def timestamp(self):
      return self._timestamp

   @property
   def unblinded_link(self):
      return self._unblinded_link

   @property
   def transacion_id(self):
      return self._tx_id

   def isPending(self):
      return self._status in [
         self.WITHDRAW_ACCEPTED,
         self.WITHDRAW_PENDING,
         self.WITHDRAW_BROADCASTED,
         self.WITHDRAW_BATCHED
      ]

   def canBeCancelled(self):
      return self._status in [
         self.WITHDRAW_ACCEPTED,
         self.WITHDRAW_PENDING,
         self.WITHDRAW_BATCHED
      ]

####
class DepositInfo():
   def __init__(self, data):
      self._tx_id = str(data['tx_id'])
      self._nb_conf = int(data['nb_conf'])
      self._unblinded_link = str(data['unblinded_link'])
      self._timestamp = datetime.fromtimestamp(data['timestamp'])
      self._outputs = data['outputs']
      self._recv_address = data['recv_address']

   @property
   def transacion_id(self):
      return self._tx_id

   @property
   def confirmations_count(self):
      return self._nb_conf

   @property
   def unblinded_link(self):
      return self._unblinded_link

   @property
   def outputs(self):
      return self._outputs

   @property
   def timestamp(self):
      return self._timestamp

   @property
   def recv_address(self):
      return self._recv_address


### history ###
class TradeHistory():
   def __init__(self, data):
      self._loaded = data['loaded']
      if self._loaded:
         self._orders = [LeverexOrder(order_data) for order_data in data['orders']]
         self._start_time = datetime.fromtimestamp(data['start_time'])
         self._end_time = datetime.fromtimestamp(data['end_time'])
      else:
         self._start_time = None
         self._end_time = None
         self._orders = None

   @property
   def loaded(self):
      return self._loaded

   @property
   def start_time(self):
      return self._start_time

   @property
   def end_time(self):
      return self._end_time

   @property
   def orders(self):
      return self._orders

### product info helper ###
class ProductInfo():
   def __init__(self, *, product_name, cash_ccy, margin_ccy, crypto_ccy, margin_rate = 10, rolling):
      self._product_name = product_name
      self._cash_ccy = cash_ccy
      self._margin_ccy = margin_ccy
      self._crypto_ccy = crypto_ccy
      self._im = margin_rate
      self._rolling = rolling

   @property
   def product_name(self):
      return self._product_name

   @property
   def cash_ccy(self):
      return self._cash_ccy

   @property
   def margin_ccy(self):
      return self._margin_ccy

   @property
   def crypto_ccy(self):
      return self._crypto_ccy

   @property
   def is_rolling(self):
      return self._rolling

   @property
   def margin_rate(self):
      return self._im

###
def get_product_info(product_name):
   if product_name == 'xbtusd_rf':
      return ProductInfo(product_name=product_name, cash_ccy='USDT', margin_ccy='USDP', crypto_ccy='XBT', rolling = True)
   if product_name == 'ethusd_rf':
      return ProductInfo(product_name=product_name, cash_ccy='USDT', margin_ccy='eth_usd', crypto_ccy='ETH', rolling = True)

   return None

###
def get_platform_products():
   return ['xbtusd_rf', 'ethusd_rf']
