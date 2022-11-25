from datetime import datetime

####
Position = 'position'
Balance = 'balance'
OrderBook = 'orderbook'
Ready = 'ready'

class ProviderException(Exception):
   pass

class OfferException(Exception):
   pass

################################################################################
class Position(object):
   def __init__(self):
      pass

################################################################################
class SessionOpenInfo():
   def __init__(self, data):
      self.product_type = data['product_type']
      self.cut_off_at = datetime.fromtimestamp(data['cut_off_at'])
      self.last_cut_off_price = float(data['last_cut_off_price'])
      self.session_id = data['session_id']
      self.previous_session_id = data['previous_session_id']

####
class SessionCloseInfo():
   def __init__(self, data):
      self.product_type = data['product_type']
      self.session_id = data['session_id']

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

   def getOpenPrice(self):
      if self.open is None:
         return 0
      return self.open.last_cut_off_price

################################################################################
class PriceOffer():
   def __init__(self, volume, ask=None, bid=None):
      if volume == 0 or (ask == 0 and bid == 0):
         raise OfferException()

      self._volume = volume
      self._ask = ask
      self._bid = bid

   @property
   def volume(self):
      return self._volume

   @property
   def ask(self):
      return self._ask

   @property
   def bid(self):
      return self._bid

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

   def compare(self, offer):
      if self._volume != offer._volume:
         return False
      if self._ask != offer._ask or self._bid != offer._bid:
         return False

      return True

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
   def __init__(self, id, timestamp, quantity, price):
      self._id = id
      self._timestamp = timestamp
      self._quantity = quantity
      self._price = price

   @property
   def id(self):
      return self._id

   @property
   def timestamp(self):
      return self._timestamp

   @property
   def is_sell(self):
      return self._quantity < 0

   @property
   def quantity(self):
      return abs(self._quantity)

   @property
   def price(self):
      return self._price


'''
class Order():
   def __init__(self, data):
      self._id = data['id']
      self._status = int(data['status'])
      self._timestamp = data['timestamp']
      self._product_type = data['product_type']
      self._side = int(data['side'])
      self._quantity = float(data['quantity'])
      self._price = float(data['price'])
      self._cut_off_price = float(data['cut_off_price'])
      self._trade_im = data['trade_im']
      self._trade_pnl = data['trade_pnl']
      self._reference_exposure = data['reference_exposure']
      self._session_id = int(data['session_id'])
      self._rollover_type = data['rollover_type']
      self._fee = data['fee']

   @property
   def id(self):
      return self._id

   @property
   def is_filled(self):
      return self._status == ORDER_STATUS_FILLED

   @property
   def timestamp(self):
      return self._timestamp

   @property
   def product_type(self):
      return self._product_type

   @property
   def is_sell(self):
      return self._side == SIDE_SELL

   @property
   def quantity(self):
      return self._quantity

   @property
   def price(self):
      return self._price

   @property
   def cut_off_price(self):
      return self._cut_off_price

   @property
   def trade_im(self):
      return self._trade_im

   @property
   def trade_pnl(self):
      return self._trade_pnl

   @property
   def total_net_exposure(self):
      if self.is_trade_position:
         return self._reference_exposure
      return self.quantity

   @property
   def session_id(self):
      return self._session_id

   @property
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
'''