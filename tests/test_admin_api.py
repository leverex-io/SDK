# env variables
# API_ENDPOINT
# API_KEY

import argparse
import asyncio
import json
import os
import random
import time
import websockets
from datetime import datetime, timedelta

from login_service_client import LoginServiceClient

async def startAdminApiTesting(phone, api_endpoint, login_endpoint, private_key_path, api_endpoint_for_challenge):
   # get sessoin
   login_client = LoginServiceClient(phone_number=phone,\
                                     login_endpoint=login_endpoint,\
                                     private_key_path=private_key_path,\
                                     dump_communication=True)

   print('Start connection to API : {}'.format(api_endpoint))
   async with websockets.connect(api_endpoint) as websocket:
      # get session token
      access_token_info = login_client.get_access_token(api_endpoint_for_challenge)

      token_validity = access_token_info['expires_in']
      expire_at = datetime.now() + timedelta(seconds=token_validity)

      token_renew_timepoint = datetime.now() + timedelta(seconds=(token_validity*0.8))

      print(f'Token received. Expire in {token_validity} at {expire_at}')

      # authorize
      print('Sending auth request')

      current_access_token = access_token_info['access_token']

      auth_request = {
         'request' : 'authorize',
         'data' : {
            'token' : current_access_token
         }
      }

      await websocket.send(json.dumps(auth_request))
      data = await websocket.recv()
      loginResult = json.loads(data)
      if not loginResult['data']['authorized']:
         print('Login failed')
         return

      # send request for a pullers snapshots
      # pullers_snaphot_request = {
      #    'request' : 'aggregation_state'
      # }
      # await websocket.send(json.dumps(pullers_snaphot_request))

      # dealers info request
      # dealers_info_request = {
      #    'request' : 'active_dealers_info'
      # }
      # await websocket.send(json.dumps(dealers_info_request))

      # session info request
      sessions_info_request = {
         'request' : 'active_sessions_info'
      }
      await websocket.send(json.dumps(sessions_info_request))

      # active users
      # active_users_request = {
      #    'request' : 'users_info'
      # }
      # await websocket.send(json.dumps(active_users_request))

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
               'request' : 'authorize',
               'data' : {
                  'token' : current_access_token
               }
            }
            await websocket.send(json.dumps(auth_request))
         if data is None:
            continue

         update = json.loads(data)
         update_type = update['response']

         if update_type == 'authorize':
            if not update['data']['authorized']:
               print('Login failed')
               return
            print('Session token renewed')
         elif update_type == 'aggregation_state':
            print('Aggregation state:\n{}'.format(update))
         elif update_type == 'active_dealers_info':
            print('Dealers info:\n{}'.format(update))
         elif update_type == 'active_sessions_info':
            print('Session info:\n{}'.format(update))
         elif update_type == 'users_info':
            print('Users info:\n{}'.format(update))
         else:
            print('Ignore update\n{}'.format(update))

if __name__ == '__main__':
   input_parser = argparse.ArgumentParser()

   input_parser.add_argument('--configs_path',
                             help='path to private key cand config file',
                             required=True,
                             action='store')

   input_parser.add_argument('--api_endpoint',
                             help='api endpoint',
                             required=True,
                             action='store')

   input_parser.add_argument('--api_endpoint_for_challenge',
                             help='api endpoint text set to challenge',
                             required=True,
                             action='store')

   input_parser.add_argument('--login_endpoint',
                             help='login service endpoint',
                             required=True,
                             action='store')


   args = input_parser.parse_args()
   with open(os.path.join(args.configs_path, 'config.json')) as json_file:
      settings = json.load(json_file)

   if 'phone' not in settings:
      print('phone not set via settings file')
      exit(1)

   api_endpoint_for_challenge = args.api_endpoint_for_challenge
   if api_endpoint_for_challenge is None or len(api_endpoint_for_challenge) == 0:
      print('API_ENDPOINT for challenge not set')
      exit(1)

   api_endpoint = args.api_endpoint
   if api_endpoint is None or len(api_endpoint) == 0:
      print('API_ENDPOINT not set')
      exit(1)

   login_endpoint = args.login_endpoint
   if login_endpoint is None or len(login_endpoint) == 0:
      print('LOGIN_SERVICE_ENDPOINT not set')
      exit(1)

   print('API api endpoint: {}'.format(api_endpoint))
   print('Login service endpoint: {}'.format(login_endpoint))
   print('Phone : {}'.format(settings['phone']))

   asyncio.get_event_loop().run_until_complete(\
      startAdminApiTesting(phone=settings['phone']\
                           , api_endpoint=api_endpoint\
                           , login_endpoint=login_endpoint\
                           , private_key_path=os.path.join(args.configs_path, 'key.pem')
                           , api_endpoint_for_challenge=api_endpoint_for_challenge))
