import logging
import asyncio
import json
import argparse
import time

from Providers.Leverex import LeverexProvider
from Providers.Bitfinex import BitfinexProvider
from Factories.Dealer.Factory import DealerFactory
from Hedger.SimpleHedger import SimpleHedger
from StatusReporter.LocalReporter import LocalReporter
from StatusReporter.WebReporter import WebReporter

#import pdb; pdb.set_trace()

################################################################################
if __name__ == '__main__':
   LOG_FORMAT = (
      "%(asctime)s,%(msecs)d %(levelname)-8s [%(filename)s:%(lineno)d] %(message)s"
   )
   logging.basicConfig(level=logging.INFO, format=LOG_FORMAT)

   parser = argparse.ArgumentParser(description='Leverex Bitfinix Dealer') 

   parser.add_argument('--config', type=str, help='Config file to use')

   args = parser.parse_args()

   config = {}
   with open(args.config) as json_config_file:
      config = json.load(json_config_file)

   while True:
      try:
         maker = LeverexProvider(config)
         taker = BitfinexProvider(config)
         hedger = SimpleHedger(config)
         statusReporter = LocalReporter(config)
         webStatusReporter = WebReporter(config)
         dealer = DealerFactory(maker, taker, hedger, [statusReporter, webStatusReporter])

         asyncio.run(dealer.run())
      except Exception as e:
         logging.error(f"!! Main loop broke with error: {str(e)} !!")
         logging.warning("!! Restarting in 5 !!")
         time.sleep(20)
