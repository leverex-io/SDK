class ProductInfo():
   def __init__(self, product_name, cash_ccy, margin_ccy, rolling):
      self._product_name = product_name
      self._cash_ccy = cash_ccy
      self._margin_ccy = margin_ccy
      self._rolling = rolling

   def product_name(self):
      return self._product_name

   def cash_ccy(self):
      return self._cash_ccy

   def margin_ccy(self):
      return self._margin_ccy

   def is_rolling(self):
      return self._rolling

def get_product_info(product_name):
   if product_name == 'xbteur_rf':
      return ProductInfo(product_name=product_name, cash_ccy='EUR', margin_ccy='EURP', rolling = True)
   if product_name == 'xbtusd_rf':
      return ProductInfo(product_name=product_name, cash_ccy='USD', margin_ccy='USDP', rolling = True)

   return None
