# env variables
# API_ENDPOINT
# API_KEY

import asyncio
import json
import time
import websockets
import websockets.exceptions
import logging
from datetime import datetime, timedelta

class AsyncApiConnection(object):
   def __init__(self, login_client, listener):

      self.login_client = login_client
      self.access_token = None
      self.websocket = None
      self.listener = None
      self.setListener(listener)

      #setup write queue
      self.write_queue = asyncio.Queue()

   def setListener(self, listener):
      self.listener = listener

      def send_data(data):
         self.write_queue.put_nowait(json.dumps(data))
      self.listener.send = send_data

   def loadBalances(self):
      loadBalanceRequest = {
         'load_balance' : {}
      }
      self.write_queue.put_nowait(json.dumps(loadBalanceRequest))

   async def login(self, service_url):
      #get token from login server
      logging.info("logging in")
      access_token_info = await self.login_client.get_access_token(service_url)

      #submit to service
      self.access_token = access_token_info
      auth_request = {
         'authorize' : {
            'token' : access_token_info['access_token']
         }
      }

      await self.websocket.send(json.dumps(auth_request))
      data = await self.websocket.recv()
      loginResult = json.loads(data)
      if not loginResult['authorize']['success']:
         raise Exception("Login failed")

      logging.info("logged in")

   async def run(self, service_url):
      async with websockets.connect(service_url) as self.websocket:
         await self.login(service_url)

         # load balances
         self.loadBalances()

         # subscribe to prices
         subscribeRequest = {
            'subscribe' : {
               'product_type' : self.listener.target_product
            }
         }
         self.write_queue.put_nowait(json.dumps(subscribeRequest))

         #start the loops
         readTask = asyncio.create_task(self.readLoop())
         writeTask = asyncio.create_task(self.writeLoop())
         cycleTask = asyncio.create_task(self.cycleSession())
         updateTask = asyncio.create_task(self.listener.updateOffer())
         await readTask
         await writeTask
         await cycleTask
         await updateTask

   async def readLoop(self):
      balance_awaitable = False
      while True:
         data = await self.websocket.recv()
         if data is None:
            continue
         update = json.loads(data)

         if 'market_data' in update:
            self.listener.onMarketData(update)

         elif 'load_balance' in update:
            self.listener.onLoadBalanceInner(update)

         elif 'subscribe' in update:
            if not update['subscribe']['success']:
               raise Exception('Failed to subscribe to prices: {}'.format(update['subscribe']['error_msg']))

         elif 'submit_prices' in update:
            self.listener.onSubmitPrices(update)

         elif 'order_update' in update:
            self.listener.onOrderUpdateInner(update)

         elif 'authorize' in update:
            if not update['authorize']['success']:
               raise Exception('Failed to renew session token')
            logging.info('Session token renewed')
         elif 'logout' in update:
            raise Exception('ERROR: we got a logout message. Closing connection')
         else:
            logging.warning('Ignore update\n{}'.format(update))

   async def writeLoop(self):
      while True:
         write_data = await self.write_queue.get()
         await self.websocket.send(write_data)

   async def cycleSession(self):
      while True:
         #wait for token lifetime - 1min
         await asyncio.sleep(self.access_token['expires_in'] * 0.9)

         #cycle token with login server
         logging.info("Updating token")
         self.access_token = await self.login_client.update_access_token(
            self.access_token['access_token'])

         #send to service
         auth_request = {
            'authorize' : {
               'token' : self.access_token['access_token']
            }
         }

         self.write_queue.put_nowait(json.dumps(auth_request))
