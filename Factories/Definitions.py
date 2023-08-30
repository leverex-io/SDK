from datetime import datetime
import time

## dealer events ##
Position = 'position'
Balance = 'balance'
OrderBook = 'orderbook'
Ready = 'ready'
Collateral = 'collateral'
PriceEvent = 'index_price'
Rebalance = 'rebalance'
Transaction = 'transaction'
##

## balance dict keys ##
kBalanceSectionKey = 'balances'
kBalanceKey = 'balance'
kMaxBuyKey = 'max_amount_buy'
kMaxSellKey = 'max_amount_sell'
kCurrencyKey = 'currency'
kQuantityKey = 'qty'
##

SIDE_BUY = 1
SIDE_SELL = 2

################################################################################
class ProviderException(Exception):
   pass

class OfferException(Exception):
   pass

################################################################################
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

################################################################################
class PriceOffer():
   def __init__(self, volume, ask=None, bid=None, isLast=False):
      self._volume = round(volume, 8)
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

################################################################################
class Offer():
   def __init__(self, price, volume):
      self._price = price
      self._volume = volume

   @property
   def price(self):
      return self._price

   @property
   def volume(self):
      return self._volume

########
class PriceBookEntry():
   def __init__(self, data):
      self._price = data[0]
      self._order_count = int(data[1])
      self._is_ask = False
      self._volume = data[2]
      if self._volume < 0:
         self._is_ask = True
         self._volume = -self._volume

   @property
   def price(self):
      return self._price

   @property
   def order_count(self):
      return self._order_count

   @property
   def is_ask(self):
      return self._is_ask

   @property
   def volume(self):
      return self._volume

########
class AggregationOrderBook():
   def __init__(self):
      self._asks = {}
      self._bids = {}

   def reset(self):
      self._asks = {}
      self._bids = {}

   def setup_from_snapshot(self, snapshot_data):
      for entry in snapshot_data:
         self._set_entry(PriceBookEntry(entry))

   def process_update(self, update):
      entry = PriceBookEntry(update)

      if entry.order_count == 0:
         self._remove_entry(entry)
      else:
         self._set_entry(entry)

   def _set_entry(self, entry: PriceBookEntry):
      if entry.is_ask:
         target_book = self._asks
      else:
         target_book = self._bids

      target_book[entry.price] = entry.volume

   def _remove_entry(self, entry: PriceBookEntry):

      if entry.is_ask:
         target_book = self._asks
      else:
         target_book = self._bids

      target_book.pop(entry.price)

   def get_aggregated_ask_price(self, target_volume):
      offers = sorted(self._asks.items())
      return self._get_aggregated_offer(offers, target_volume)

   def get_aggregated_bid_price(self, target_volume):
      offers = sorted(self._bids.items(), reverse=True)
      return self._get_aggregated_offer(offers, target_volume)

   def _get_aggregated_offer(self, offers, target_volume):
      if target_volume == 0:
         return Offer(0, 0)

      total_volume = 0
      total_cost = 0

      if len(offers) == 0:
         return None

      for offer in offers:
         price = offer[0]
         volume = offer[1]
         cost = volume * price

         total_volume += volume
         total_cost += cost

         if total_volume > target_volume:
            break

      final_cost = round(total_cost / total_volume, 2)
      return Offer(final_cost, total_volume)

   def __str__(self):
      return f'ask {sum(self._asks.values())}, bids {sum(self._bids.values())}'

   def pretty_print(self):
      print ("asks:")
      offers = sorted(self._asks.items(), reverse=True)
      for offer in offers:
         print(f"  - price: {offer[0]}, vol: {offer[1]}")

      print ("bids:")
      offers = sorted(self._bids.items(), reverse=True)
      for offer in offers:
         print(f"  - price: {offer[0]}, vol: {offer[1]}")

################################################################################
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

################################################################################
class PositionsReport(object):
   def __init__(self, provider):
      self.name = provider.name
      self.netExposure = provider.getExposure()
      self.openPrice = provider.getOpenPrice()
      self.indexPrice = provider.indexPrice

      if isinstance(self.netExposure, float):
         self.netExposure = round(self.netExposure, 8)
      if isinstance(self.openPrice, float):
         self.openPrice = round(self.openPrice, 2)
      if isinstance(self.indexPrice, float):
         self.indexPrice = round(self.indexPrice, 2)

   @property
   def timestamp(self):
      return self._timestamp

   def __str__(self):
      return ""

   def __eq__(self, obj):
      if not isinstance(obj, PositionsReport):
         return False
      return self.netExposure == obj.netExposure

   def getPnl(self):
      return "N/A"

   def getPnlReport(self):
      result = f" $    <{self.name} - pnl: {self.getPnl()}"
      result += f" - open price: {self.openPrice}, index price: {self.indexPrice}>"
      return result

################################################################################
class BalanceReport(object):
   def __init__(self, provider):
      self.name = provider.name
      self._timestamp = time.time_ns() / 1000000

   @property
   def timestamp(self):
      return self._timestamp

   def __eq__(self, obj):
      if not isinstance(obj, BalanceReport):
         return False

      #5min sec intervals
      return abs(obj._timestamp - self._timestamp) <= 300000

################################################################################
class RebalanceReport(object):
   def __init__(self, provider):
      self.name = provider.name
      self._timestamp = time.time_ns() / 1000000

   @property
   def timestamp(self):
      return self._timestamp

   def __eq__(self, obj):
      if not isinstance(obj, RebalanceReport):
         return False

      #1min sec intervals
      return abs(obj._timestamp - self._timestamp) <= 30000

   def __str__(self):
      return "N/A"

################################################################################
class DepositWithdrawAddresses():
   def __init__(self):
      self._deposit_address = None
      self._withdraw_address = None
      self._default_withdraw_addr = None

   ## get
   def getDepositAddr(self):
      return self._deposit_address

   def getWithdrawAddresses(self):
      if not self.hasWithdrawAddr():
         raise Exception("missing withdraw address")
      return self._withdraw_address

   def getDefaultWithdrawAddr(self):
      if self._default_withdraw_addr == None:
         raise Exception("missing default withdraw address")
      return self._default_withdraw_addr

   ## set
   def setDepositAddr(self, address):
      self._deposit_address = address

   def setWithdrawAddresses(self, addresses):
      self._withdraw_address = addresses

   def setDefaultWithdrawAddr(self, addr):
      if addr in self._withdraw_address:
         self._default_withdraw_addr = addr

   ## has
   def hasDepositAddr(self):
      if self._deposit_address == None or \
         len(self._deposit_address) == 0:
         return False
      return True

   def hasWithdrawAddr(self):
      if self._withdraw_address == None or \
         len(self._withdraw_address) == 0:
         return False
      return True

   def hasAddresses(self):
      return self.hasDepositAddr() and self.hasWithdrawAddr()

   def hasDefaultWtdrAddr(self):
      return self._default_withdraw_addr != None

################################################################################
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

################################################################################
class CashOperation(object):
   INIT            = 1
   SETUP           = 2
   READY           = 3
   PERFORMING_TASK = 10
   MONITORING_TASK = 11
   DONE            = 20

   def __init__(self):
      self.state = self.INIT
      self._id = None

   def id(self):
      return self._id

   def setId(self, val):
      if self.id() != None:
         raise ProviderException("op has id")
      self._id = val

   def done(self):
      return self.state == self.DONE

   async def doTheTask(self, provider):
      #implement me
      pass

   async def setup(self, provider):
      #implement me
      pass

   def assessProgress(self, provider):
      #implement me
      return False

   async def process(self, provider):
      state = self.state
      if state == self.INIT:
         self.state = self.SETUP
         success = self.setup(provider)

         if success == True:
            self.state = self.READY
            await self.process(provider)
         elif success == False:
            self.state = self.INIT
         elif success == None:
            self.state = self.DONE

      elif state == self.READY:
         self.state = self.PERFORMING_TASK
         success = await self.doTheTask(provider)

         if success == True:
            self.state = self.MONITORING_TASK
         elif success == False:
            self.state = self.READY
         return

      elif state == self.MONITORING_TASK:
         if self.assessProgress(provider):
            self.state = self.DONE
         return

   def stageStr(self):
      if self.state == self.INIT:
         return "INIT"
      elif self.state == self.SETUP:
         return "SETUP"
      elif self.state == self.READY:
         return "READY"
      elif self.state == self.PERFORMING_TASK:
         return "PERFORMING TASK"
      elif self.state == self.MONITORING_TASK:
         return "MONITORING TASK"
      elif self.state == self.DONE:
         return "DONE"
      return "N/A"

   def __str__(self):
      return "N/A"

   def __eq__(self, other):
      if not isinstance(other, CashOperation):
         return False
      raise Exception("implement me!")

################################################################################
class SideVolume(object):
   def __init__(self, balance, margin, price):
      self.freeBalance = balance
      self.freeMargin = margin
      self.priceFactor = price

   def getOpenVolume(self, maxVolume, unquoteRatio):
      #this is the max balance the dealer is allowed to quote
      maxBalance = maxVolume * self.priceFactor

      #this is the balance the provider has avaible to quote,
      #capped by max dealer volume
      openBalance = min(maxBalance, self.freeBalance)

      #this is the balance the provider is allowed to quote, as
      #defined by the portion of balance it has to keep unemcumbered
      #for rebalancing purposes
      quotableBalance = self.freeBalance * (1-unquoteRatio)

      #keep the smallest of the 2 as our balance to quote
      balanceToQuote = min(openBalance, quotableBalance)

      #finally, undiscriminately add free margin, as it is
      #balance freed by reducing position on the opposite side
      #margin freed on one side can be reused on the other,
      #hence the 2x
      return (balanceToQuote + self.freeMargin * 2) / self.priceFactor

########
class OpenVolume(object):
   def __init__(self, balance, askMargin, askPrice, bidMargin, bidPrice):

      #ask side should know margin held in bids and vice versa
      #this is because margin is freed by increasing exposure
      #in the opposite side
      self.ask = SideVolume(balance, bidMargin, askPrice)
      self.bid = SideVolume(balance, askMargin, bidPrice)

   def get(self, maxVolume, unquoteRatio):
      result = {
         'ask': self.ask.getOpenVolume(maxVolume, unquoteRatio),
         'bid': self.bid.getOpenVolume(maxVolume, unquoteRatio)
      }
      return result

########
def getBalancesFromJson(jsonDict):
   result = {}
   if kBalanceSectionKey in jsonDict:
      for account in jsonDict[kBalanceSectionKey]:
         if not kBalanceKey in account or not kCurrencyKey in account:
            continue
         result[account[kCurrencyKey]] = float(account[kBalanceKey])

   return result

################################################################################
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

########
class OnChainTransaction(object):
   def __init__(self, txid, recipient, nConf, outputs):
      self._id = txid
      self.recipient = recipient
      self.nConf = nConf
      self.outputs = outputs

   def __eq__(self, other):
      if not isinstance(other, OnChainTransaction):
         return False
      return self.id == other.id

   @property
   def id(self):
      return self._id

########
class TransactionTracker(object):
   def __init__(self):
      self.transactions = {}
      self.orderedByTimestamp = {}

   def addTransaction(self, txid, recipient, nConf, outputs):
      if txid in self.transactions:
         self.transactions[txid].nConf = nConf
      else:
         self.transactions[txid] = OnChainTransaction(
            txid, recipient, nConf, outputs)
         now = round(time.time() * 1000)
         self.orderedByTimestamp[now] = txid

   def addDeposit(self, deposit):
      self.addTransaction(deposit.transaction_id,
         deposit.recv_address,
         deposit.confirmations_count,
         deposit.outputs)

   def getTransactionsSince(self, timestamp=0):
      #i hate python =)
      txids = [v for k, v in self.orderedByTimestamp.items() if k >= timestamp]
      return [tx for id, tx in self.transactions.items() if id in txids]

   def getTx(self, txId):
      if txId not in self.transactions:
         return None
      return self.transactions[txId]

TheTxTracker = TransactionTracker()