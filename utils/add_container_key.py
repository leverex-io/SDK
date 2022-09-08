import argparse
import os.path
import sys
import json
import asyncio
import logging

sys.path.append('..')

from trader_core.login_connection import LoginServiceClientWS


async def add_container_keys_to_env_autheid(keys_path):
   settings_file = os.path.join(keys_path, 'config.json')

   with open(settings_file, 'r') as settings_file:
      settings = json.load(settings_file)

   login_client = LoginServiceClientWS(email=settings['email'],
                                     login_endpoint=settings['login_endpoint'],
                                     private_key_path=os.path.join(keys_path, 'key.pem'),
                                     dump_communication=False)

   await login_client.send_key_to_endpoint()

if __name__ == '__main__':
   input_parser = argparse.ArgumentParser()

   input_parser.add_argument('--keys_path',
                             help='path to keys storage',
                             required=True,
                             action='store')

   args = input_parser.parse_args()

   log_level = 'INFO'

   logging.basicConfig(level=log_level)

   asyncio.get_event_loop().run_until_complete(add_container_keys_to_env_autheid(keys_path=args.keys_path))
   print('Key was added successfully')
