# env variables
# API_ENDPOINT
# API_KEY

import asyncio
import json
import os
import random
import time
import websockets

from datetime import datetime, timedelta

from login_service_client import LoginServiceClient
from product_mapping import get_product_info

from influxdb_client import InfluxDBClient, Point
from influxdb_client.client.write_api import SYNCHRONOUS

async def startTrading(phone, api_endpoint, login_endpoint):
   # validate that we have all required data set
   db_host = os.environ.get('DB_HOST')
   if db_host is None or len(db_host) == 0:
      print('ERROR: DB_HOST is not set')
      return
   db_org = os.environ.get('DB_ORG')
   if db_org is None or len(db_org) == 0:
      print('ERROR: DB_ORG is not set')
      return
   db_bucket = os.environ.get('DB_BUCKET')
   if db_bucket is None or len(db_bucket) == 0:
      print('ERROR: DB_BUCKET is not set')
      return
   db_token = os.environ.get('DB_TOKEN')
   if db_token is None or len(db_token) == 0:
      print('ERROR: DB_TOKEN is not set')
      return
   env_name = os.environ.get('ENV_NAME')
   if env_name is None or len(env_name) == 0:
      print('ERROR: ENV_NAME is not set')
      return

   min_cash_amount_str = os.environ.get('MIN_CASH_AMOUNT')
   if min_cash_amount_str is None or len(min_cash_amount_str) == 0:
      print('ERROR: MIN_CASH_AMOUNT is not set')
      return

   min_cash_amount = int(min_cash_amount_str)

   target_product = os.environ.get('TARGET_PRODUCT')
   if target_product is None or len(target_product) == 0:
      print('ERROR: TARGET_PRODUCT is not set')
      return

   product_info = get_product_info(target_product)
   if product_info is None:
      print(f'ERROR: no mapping for product {target_product}')
      return

   default_trade_send_interval = os.environ.get('ORDER_SEND_INTERVAL')
   if env_name is None or len(env_name) == 0:
      default_trade_send_interval = 10
   else:
      default_trade_send_interval = int(default_trade_send_interval)

   client = InfluxDBClient(url=db_host, token=db_token, org=db_org)
   write_api = client.write_api(write_options=SYNCHRONOUS)

   login_client = LoginServiceClient(phone_number=phone,\
                                     login_endpoint=login_endpoint,\
                                     private_key_path='/usr/app/keys/key.pem',\
                                     dump_communication=False)

   print('Start connection to API : {}'.format(api_endpoint))
   async with websockets.connect(api_endpoint) as websocket:
      # get session token
      access_token_info = login_client.get_access_token(api_endpoint)

      token_validity = access_token_info['expires_in']
      expire_at = datetime.now() + timedelta(seconds=token_validity)

      token_renew_timepoint = datetime.now() + timedelta(seconds=(token_validity*0.8))

      print(f'Token received. Expire in {token_validity} at {expire_at}')

      # authorize
      print('Sending auth request')

      current_access_token = access_token_info['access_token']

      auth_request = {
         'authorize' : {
            'token' : current_access_token
         }
      }

      await websocket.send(json.dumps(auth_request))
      data = await websocket.recv()
      loginResult = json.loads(data)
      if not loginResult['authorize']['success']:
         print('Login failed')
         return

      session_validity_time = loginResult['authorize']['validity']
      session_expire_time = datetime.now() + timedelta(seconds=session_validity_time)

      print(f'Authorized. Session expire in {session_validity_time} at {session_expire_time}')

      # load balances
      loadBalanecRequest = {
         "load_balance" : {}
      }
      await websocket.send(json.dumps(loadBalanecRequest))

      # subscribe to prices
      subscribeRequest = {
         'subscribe' : {
            'product_type' : target_product
         }
      }

      print(f'Subscribe to market prices for {target_product}')
      await websocket.send(json.dumps(subscribeRequest))

      baseAmount = '0.001'
      sideIndex = 0
      sideStr = [2, 1]
      extraAmountRange=100000
      prices = [None, None]

      # orderSendInterval in seconds
      orderSendInterval = default_trade_send_interval
      print(f'Orders will be sent with random interval based on {orderSendInterval}')

      failedOrdersCount = 0
      maxFailedOrdersCount = 6

      nextOrderTime = time.time() + orderSendInterval
      orderPending = False
      balance_loaded = False

      while True:
         try:
            data = await asyncio.wait_for(websocket.recv(), timeout=1.0)
         except asyncio.TimeoutError:
            data = None

         if datetime.now() > token_renew_timepoint:
            # get new access token
            renew_result = login_client.update_access_token(current_access_token)
            current_access_token = renew_result['access_token']

            # renew expire time
            token_validity = renew_result['expires_in']
            token_renew_timepoint = datetime.now() + timedelta(seconds=(token_validity*0.8))

            print(f'Token renewed. Next renew at {token_renew_timepoint}')
            print('Updating API connection')
            auth_request = {
               'authorize' : {
                  'token' : current_access_token
               }
            }
            await websocket.send(json.dumps(auth_request))
         if data is None:
            continue
         update = json.loads(data)

         if 'market_data' in update:
            ask = update['market_data']['ask']
            bid = update['market_data']['bid']
            prices = [bid, ask]
            # print(f'Price updated: {bid} : {ask}')
         elif 'load_balance' in update:
            for balanceInfo in update['load_balance']['balances']:
               print('Balance updated: {} {}'.format(balanceInfo['balance'], balanceInfo['currency']))
               if balanceInfo['currency'] == product_info.cash_ccy():
                  if float(balanceInfo['balance']) < min_cash_amount:
                     print(f'{product_info.cash_ccy()} balance is too small. Need {min_cash_amount}')
                     # get deposit ref
                     get_deposit_ref = {
                        'get_deposit_info' : {}
                     }

                     await websocket.send(json.dumps(get_deposit_ref))
                  else:
                     balance_loaded = True
         elif 'get_deposit_info' in update:
            print('Deposit info:{}'.format(update['get_deposit_info']))
            return
         elif 'subscribe' in update:
            if not update['subscribe']['success']:
               print('Failed to subscribe to prices: {}'.format(update['subscribe']['error_msg']))
               return
         elif 'order_update' in update:
            print('Order update: {}'.format(update))
            pass
         elif 'market_order' in update:
            # print('Order response: {}'.format(update))
            orderPending = False

            if update['market_order']['success']:
               nextOrderDelay = random.randrange(orderSendInterval)
               print('Order created. Next order in {}'.format(nextOrderDelay))
               failedOrdersCount = 0
               orderCreated = True
               createOrderEnd = time.time()
               orderProcessingTime = createOrderEnd - createOrderStart
               print('Create order request processing time {}'.format(orderProcessingTime))
               p = Point("order_processing").tag("env", env_name).tag("product", target_product).field("processing_time", orderProcessingTime)
               write_api.write(bucket=db_bucket, record=p)
            else:
               failedOrdersCount = failedOrdersCount + 1
               if failedOrdersCount >= maxFailedOrdersCount:
                  print('ERROR: To much failed orders')
                  return
               nextOrderDelay = failedOrdersCount * orderSendInterval
               print('Order create failed: {}. Next order in {} s'.format(update['market_order']['error_msg'], nextOrderDelay))

            nextOrderTime = time.time() + nextOrderDelay
         elif 'authorize' in update:
            if not update['authorize']['success']:
               print('Failed to renew session token')
               return
            print('Session token renewed')
         elif 'logout' in update:
            print('ERROR: we got logout message. Closing connetion')
            return
         else:
            print('Ignore update\n{}'.format(update))

         if time.time() > nextOrderTime and not orderPending:
            if not balance_loaded:
               print(f'ERROR: target balance not loaded: {product_info.cash_ccy()}')
               return
            expectedPrice = prices[sideIndex]
            if expectedPrice is None:
               print('ERROR: Market price is undefined. Could not create an order')
               continue

            if expectedPrice == 0:
               continue

            tradeExtraAmount = random.randrange(extraAmountRange)
            tradeAmountStr = f'{baseAmount}{tradeExtraAmount:05d}'
            side = sideStr[sideIndex]

            # reset prices
            prices = [None, None]

            # change side
            sideIndex = (sideIndex + 1) % 2

            print(f'Submitting order {side} for {tradeAmountStr}@{expectedPrice}')

            createOrderRequest = {
               'market_order' : {
               'amount'                : tradeAmountStr,
               'user_expected_price'   : expectedPrice,
               'side'                  : side,
               'product_type'          : target_product
               }
            }
            orderCreated = False
            orderPending = True
            createOrderStart = time.time()

            await websocket.send(json.dumps(createOrderRequest))
            continue

if __name__ == "__main__":
   # load settings
   with open('/usr/app/keys/config.json') as json_file:
      settings = json.load(json_file)

   if 'phone' not in settings:
      print('phone not set via settings file')
      exit(1)

   api_endpoint = os.environ.get('API_ENDPOINT')
   if api_endpoint is None or len(api_endpoint) == 0:
      if 'api_endpoint' not in settings:
         print('API_ENDPOINT not set')
         exit(1)
      api_endpoint = settings['api_endpoint']

   login_endpoint = os.environ.get('LOGIN_SERVICE_ENDPOINT')
   if login_endpoint is None or len(login_endpoint) == 0:
      if 'login_endpoint' not in settings:
         print('LOGIN_SERVICE_ENDPOINT not set')
         exit(1)
      login_endpoint = settings['login_endpoint']

   print('API api endpoint: {}'.format(api_endpoint))
   print('Login service endpoint: {}'.format(login_endpoint))
   print('Phone : {}'.format(settings['phone']))

   asyncio.get_event_loop().run_until_complete(startTrading(phone=settings['phone'], api_endpoint=api_endpoint, login_endpoint=login_endpoint))
