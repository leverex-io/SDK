import unittest

from Factories.Definitions import AggregationOrderBook

################################################################################
##
#### Order book tests
##
################################################################################
class TestOrderBook(unittest.TestCase):
   def test_price_agg(self):
      orderBook = AggregationOrderBook()

      ## asks, price should be above the index price ##
      orderBook.process_update([10010, 1, -1])
      orderBook.process_update([10050, 1, -2])
      orderBook.process_update([10100, 1, -5])

      #0.5
      result = orderBook.get_aggregated_ask_price(0.5)
      self.assertEqual(result.price, 10010)
      self.assertEqual(result.volume, 1)

      #1
      result = orderBook.get_aggregated_ask_price(1)
      self.assertEqual(result.price, 10036.67)
      self.assertEqual(result.volume, 3)

      #2
      result = orderBook.get_aggregated_ask_price(2)
      self.assertEqual(result.price, 10036.67)
      self.assertEqual(result.volume, 3)

      #5
      result = orderBook.get_aggregated_ask_price(5)
      self.assertEqual(result.price, 10076.25)
      self.assertEqual(result.volume, 8)

      #7
      result = orderBook.get_aggregated_ask_price(7)
      self.assertEqual(result.price, 10076.25)
      self.assertEqual(result.volume, 8)

      #10
      result = orderBook.get_aggregated_ask_price(10)
      self.assertEqual(result.price, 10076.25)
      self.assertEqual(result.volume, 8)

      ## bids, price should be below the index price ##
      orderBook.process_update([9990, 1, 1])
      orderBook.process_update([9950, 1, 2])
      orderBook.process_update([9900, 1, 5])

      #0.5
      result = orderBook.get_aggregated_bid_price(0.5)
      self.assertEqual(result.price, 9990)
      self.assertEqual(result.volume, 1)

      #1
      result = orderBook.get_aggregated_bid_price(1)
      self.assertEqual(result.price, 9963.33)
      self.assertEqual(result.volume, 3)

      #2
      result = orderBook.get_aggregated_bid_price(2)
      self.assertEqual(result.price, 9963.33)
      self.assertEqual(result.volume, 3)

      #5
      result = orderBook.get_aggregated_bid_price(5)
      self.assertEqual(result.price, 9923.75)
      self.assertEqual(result.volume, 8)

      #7
      result = orderBook.get_aggregated_bid_price(7)
      self.assertEqual(result.price, 9923.75)
      self.assertEqual(result.volume, 8)

      #10
      result = orderBook.get_aggregated_bid_price(10)
      self.assertEqual(result.price, 9923.75)
      self.assertEqual(result.volume, 8)

################################################################################
if __name__ == '__main__':
   unittest.main()