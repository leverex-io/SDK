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

def get_product_info(product_name):
   if product_name == 'xbtusd_rf':
      return ProductInfo(product_name=product_name, cash_ccy='USD', margin_ccy='USDP', crypto_ccy='XBT', rolling = True)
   if product_name == 'ethusd_rf':
      return ProductInfo(product_name=product_name, cash_ccy='USD', margin_ccy='eth_usd', crypto_ccy='ETH', rolling = True)

   return None

def get_platform_products():
   return ['xbtusd_rf', 'ethusd_rf']
