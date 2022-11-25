import logging
import os

from trader_core.product_mapping import get_product_info

################################################################################
class MarketEventListener(object):
   target_product = 'xbtusd_rf'
   min_cash_amount = 100

   #############################################################################
   def __init__(self):
      self.balance_awaitable = False
      self.freeCash = 0.0

      #config
      self.min_cash_amount = min_cash_amount
      min_cash_amount_str = os.environ.get('MIN_CASH_AMOUNT')
      if min_cash_amount_str is not None and len(min_cash_amount_str) != 0:
         self.min_cash_amount = int(min_cash_amount_str)

      target_product_from_env = os.environ.get('TARGET_PRODUCT')
      self.target_product = target_product
      if target_product_from_env is not None and len(target_product_from_env) != 0:
         self.target_product = target_product_from_env

      self.product_info = get_product_info(self.target_product)
      if self.product_info is None:
         logging.error(f'ERROR: no mapping for product {self.target_product}')
         return

      logging.info("Dealing for product {}, with min cash set to: {}".format(
         self.target_product, self.min_cash_amount))

   #############################################################################
   def send(self, data):
      # will be set by API connection
      pass

   def on_authorized(self):
      self.subscribe_to_product(self.target_product)

   def subscribe_to_product(self, target_product):
      subscribeRequest = {
         'subscribe' : {
            'product_type' : target_product
         }
      }
      self.send(subscribe)

   #############################################################################
   def sendOffer(self, offers):
      prices = []
      for offer in offers:
         prices.append(offer)

      submit_prices_request = {
         'submit_prices' : {
            'product_type' : self.target_product,
            'prices' : prices
         }
      }

      self.send(submit_prices_request)

   #############################################################################
   def on_market_data(self, data):
      #override me
      pass

   #############################################################################
   def onLoadBalance(self, balances):
      #override me
      for balanceInfo in balances:
         logging.info('Balance updated: {} {}'.format(balanceInfo['balance'], balanceInfo['currency']))
         if balanceInfo['currency'] == self.product_info.cash_ccy():
            self.freeCash = float(balanceInfo['balance'])
            if self.freeCash < self.min_cash_amount:
               logging.error(f'{self.product_info.cash_ccy()} balance is too small. Min amount {self.min_cash_amount}')
            else:
               self.balance_awaitable = True

   #############################################################################
   def onSubmitPrices(self, data):
      result = data['submit_prices']['result']
      if result == 'SubmittedPricesRejected':
         rejectReason = data['submit_prices']['reject_reason']
         # ignore reject if trading was closed
         if rejectReason == 2:
            return

         raise Exception('Submit prices failed with reason : {}'.format(rejectReason))

   #############################################################################
   def on_order_created(self, orders):
      #override me
      pass

   def on_order_filled(self, orders):
      #override me
      pass
