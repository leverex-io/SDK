# simple client to to monitor session open/close notifications and indicative prices

import asyncio
import logging
import functools
import sys
from datetime import datetime

sys.path.append('..')

from trader_core.api_connection import AsyncApiConnection

class SessionMonitor():
   def __init__(self):
      self._api_connection = AsyncApiConnection(api_endpoint='wss://api-dev.leverex.io')

   async def run(self):
      await self._api_connection.run(self)

   def on_connected(self):
      print('Connected to leverex')
      asyncio.create_task(self._api_connection.subscribe_to_product('xbtusd_rf'))
      asyncio.create_task(self._api_connection.subscribe_session_open('xbtusd_rf'))

   def on_market_data(self, update):
      print('on_market_data: live cutoff {}, indicative prices {} : {}'.format(update['live_cutoff'], update['bid'], update['ask']))

   def on_session_open(self, update):
      print('Session opened: {} ( {} ) @ {}. Will be closed at {}'.format(update['product_type'],
                                                                          update['session_id'],
                                                                          update['last_cut_off_price'],
                                                                          datetime.fromtimestamp(update['cut_off_at'])))
   def on_session_closed(self, update):
      print('Session {} ( {} ) closed'.format(update['product_type'], update['session_id']))

def main():
   monitor = SessionMonitor()
   asyncio.run(monitor.run())

if __name__ == '__main__':
   logging.basicConfig(level='INFO')
   main()
