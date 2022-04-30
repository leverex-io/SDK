import argparse
import os.path
import sys
import json
import asyncio

from login_connection import LoginServiceClientWS

def add_container_keys_to_env(keys_path, skip_sms):
   settings_file=os.path.join(keys_path, 'config.json')

   with open(settings_file, 'r') as settings_file:
      settings = json.load(settings_file)

   login_client = LoginServiceClient(phone_number=settings['phone'],\
                                     login_endpoint=settings['login_endpoint'],\
                                     private_key_path=os.path.join(keys_path, 'key.pem'),\
                                     dump_communication=False)

   submit_result = login_client.send_key_to_endpoint()
   if skip_sms:
      sms_code='1111'
   else:
      sms_code = input("Key submitted. Please enter SMS verification code : ")
   if not login_client.confirm_key_submit(sms_code=sms_code):
      print('Failed to add key')
      exit(1)
   print('Key was added successfully')
   exit(0)

async def add_container_keys_to_env_autheid(keys_path):
   settings_file=os.path.join(keys_path, 'config.json')

   with open(settings_file, 'r') as settings_file:
      settings = json.load(settings_file)

   login_client = LoginServiceClientWS(email=settings['email'],\
                                     login_endpoint=settings['login_endpoint'],\
                                     private_key_path=os.path.join(keys_path, 'key.pem'),\
                                     dump_communication=False)

   await login_client.send_key_to_endpoint()
   exit(0)

if __name__ == '__main__':
   input_parser = argparse.ArgumentParser()

   input_parser.add_argument('--keys_path',
                             help='path to keys storage',
                             required=True,
                             action='store')

   input_parser.add_argument('--skip_sms',
                             help='For DEV env SMS validation is disabled and account creation could be automated',
                             action='store_true',
                             required=False,
                             default=False)

   input_parser.add_argument('--autheid',
                             help='Vet key through autheid (identified by email). Overrules SMS settings',
                             action='store_true',
                             required=False,
                             default=False)

   args = input_parser.parse_args()

   if args.autheid == True:
      asyncio.get_event_loop().run_until_complete(
         add_container_keys_to_env_autheid(keys_path=args.keys_path))
      print('Key was added successfully')
      sys.exit()
   else:
      sys.exit(add_container_keys_to_env(keys_path=args.keys_path, skip_sms=args.skip_sms))
