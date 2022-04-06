import argparse
import os.path
import sys
import json
import asyncio

from login_service_client import LoginServiceClientWS

async def run_it(keys_path):
    settings_file=os.path.join(keys_path, 'config.json')

    with open(settings_file, 'r') as settings_file:
        settings = json.load(settings_file)

    login_client = LoginServiceClientWS(email=settings['email'],\
                                        login_endpoint=settings['login_endpoint'],\
                                        private_key_path=os.path.join(keys_path, 'key.pem'),\
                                        dump_communication=True)

    token = await login_client.get_access_token("wss://rollthecoin-dev.blocksettle.com:443")
    print (token)

if __name__ == '__main__':
    input_parser = argparse.ArgumentParser()

    input_parser.add_argument('--keys_path',
                            help='path to keys storage',
                            required=True,
                            action='store')
    args = input_parser.parse_args()

    asyncio.get_event_loop().run_until_complete(run_it(args.keys_path))
