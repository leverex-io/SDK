import logging
import asyncio
import json

from Providers.Leverex import LeverexProvider
from Providers.Bitfinex import BitfinexProvider
from Factories.Dealer.Factory import DealerFactory
from Hedger.SimpleHedger import SimpleHedger
from StatusReporter.LocalReporter import LocalReporter

#import pdb; pdb.set_trace()

################################################################################
if __name__ == '__main__':
   LOG_FORMAT = (
      "%(asctime)s,%(msecs)d %(levelname)-8s [%(filename)s:%(lineno)d] %(message)s"
   )
   logging.basicConfig(level=logging.INFO, format=LOG_FORMAT)

   config = {}
   with open("refactored_config.json") as json_config_file:
      config = json.load(json_config_file)

   maker = LeverexProvider(config)
   taker = BitfinexProvider(config)
   hedger = SimpleHedger(config)
   statusReporter = LocalReporter()
   dealer = DealerFactory(maker, taker, hedger, [statusReporter])

   asyncio.run(dealer.run())