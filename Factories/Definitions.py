from datetime import datetime
import time

####
Position = 'position'
Balance = 'balance'
OrderBook = 'orderbook'
Ready = 'ready'

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
   def __init__(self, volume, ask=None, bid=None):
      if volume == 0 or (ask == 0 and bid == 0):
         raise OfferException()

      self._volume = volume
      self._ask = ask
      self._bid = bid
      self._timestamp = time.time_ns() / 1000000 #time in ms

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

   def compare(self, offer, delay_ms):
      if offer._timestamp <= self._timestamp + delay_ms:
         #return false if delay is met
         return False

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

################################################################################
class PositionsReport(object):
   def __init__(self, provider):
      self.name = provider.name
      self.netExposure = provider.getExposure()
      self._timestamp = time.time_ns() / 1000000

   @property
   def timestamp(self):
      return abs(self._timestamp)

   def __str__(self):
      return ""

   def __eq__(self, obj):
      if not isinstance(obj, PositionsReport):
         return False

      if self.netExposure != obj.netExposure:
         return False

      #10 sec intervals
      return abs(obj._timestamp - self._timestamp) <= 10000
