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

from login_service_client import LoginServiceClientWS, LoginServiceClient
from product_mapping import get_product_info

async def startTrading(login_client, api_endpoint, login_endpoint):
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

   # get session
   print('Start connection to API : {}'.format(api_endpoint))
   async with websockets.connect(api_endpoint) as websocket:
      # get session token
      access_token_info = await login_client.get_access_token(api_endpoint)

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
         'load_balance' : {}
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

      baseAmount = '1'
      sideIndex = 0

      balance_awailable = False

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
            cutoff_price = float(update['market_data']['live_cutoff'])
            if cutoff_price == 0:
               continue

            if not balance_awailable:
               continue

            ask = cutoff_price + 50
            bid = cutoff_price - 50

            tight_ask = cutoff_price + 30 + random.randrange(10)
            tight_bid = cutoff_price - 30 - random.randrange(10)

            # print(f'Sending price update: {bid} : {ask}')

            submit_prices_request = {
               'submit_prices' : {
                  'product_type' : target_product,
                  'prices' : [
                     {
                        'volume' : '0.1',
                        'ask'    : str(tight_ask),
                        'bid'    : str(tight_bid)
                     },
                     {
                        'volume' : '1',
                        'ask'    : str(ask),
                        'bid'    : str(bid)
                     }
                  ]
               }
            }

            await websocket.send(json.dumps(submit_prices_request))
         elif 'load_balance' in update:
            for balanceInfo in update['load_balance']['balances']:
               print('Balance updated: {} {}'.format(balanceInfo['balance'], balanceInfo['currency']))
               if balanceInfo['currency'] == product_info.cash_ccy():
                  if float(balanceInfo['balance']) < min_cash_amount:
                     print(f'{product_info.cash_ccy()} balance is too small. Min amount {min_cash_amount}')
                     # get deposit ref
                     get_deposit_ref = {
                        'get_deposit_info' : {}
                     }

                     await websocket.send(json.dumps(get_deposit_ref))
                  else:
                     balance_awailable = True
         elif 'get_deposit_info' in update:
            print('Deposit info:{}'.format(update['get_deposit_info']))
            return
         elif 'subscribe' in update:
            if not update['subscribe']['success']:
               print('Failed to subscribe to prices: {}'.format(update['subscribe']['error_msg']))
               return
         elif 'submit_prices' in update:
            result = update['submit_prices']['result']
            if result == 'SubmittedPricesRejected':
               rejectReason = update['submit_prices']['reject_reason']
               # ignore reject if trading was closed
               if rejectReason == 2:
                  continue
               print('Submit prices failed with reason : {}'.format(rejectReason))
               return
         elif 'order_update' in update:
            print('Order update: {}'.format(update))
            # order updated. Ignore
            pass
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

if __name__ == '__main__':
   # load settings
   with open('/usr/app/keys/config.json') as json_file:
      settings = json.load(json_file)

   auth_type_string = None

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

   if 'phone' in settings:
      phone_number = settings['phone']
      auth_type_string = f'Phone: {phone_number}'
      login_client = LoginServiceClient(phone_number=phone_number,\
                                        login_endpoint=login_endpoint,\
                                        private_key_path='/usr/app/keys/key.pem',\
                                        dump_communication=True)
   elif 'email' in settings:
      dealer_email = settings['email']
      auth_type_string = f'Email: {dealer_email}'
      login_client = LoginServiceClientWS(email=dealer_email,\
                                        login_endpoint=login_endpoint,\
                                        private_key_path='/usr/app/keys/key.pem',\
                                        dump_communication=True)
   else:
      print('Auth method undefined')
      exit(1)

   print('API api endpoint: {}'.format(api_endpoint))
   print('Login service endpoint: {}'.format(login_endpoint))
   print(auth_type_string)

   asyncio.get_event_loop().run_until_complete(startTrading(login_client, api_endpoint=api_endpoint, login_endpoint=login_endpoint))
