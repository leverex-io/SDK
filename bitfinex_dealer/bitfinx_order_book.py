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


class AggregationOrderBook():
   def __init__(self):
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
         return None

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

      return Offer(total_cost / total_volume, total_volume)

   def __str__(self):
      return f'ask {sum(self._asks.values())}, bids {sum(self._bids.values())}'
