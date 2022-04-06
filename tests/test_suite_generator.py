import argparse
import json
import os
import yaml

def generate_suite(configs_path):
   with open(configs_path, 'r') as data:
      configs = json.load(data)

   # check for required settings
   required_settings = ['environment',\
                     'db_token',\
                     'order_send_interval',\
                     'api_endpoint',\
                     'login_endpoint','trading_clients',\
                     'dealing_clients',\
                     'dealing_client_image',\
                     'trading_client_image',\
                     'target_product',\
                     'min_cash_amount']
   for rs in required_settings:
      if rs not in configs:
         print(f'Error: {rs} not defined')
         exit(1)

   compose_file = {}
   compose_file['version']='3.9'

   services = {}

   trading_clients_count = int(configs['trading_clients'])
   min_cash_amount = int(configs['min_cash_amount'])

   volumes = {}

   # add influx

   if trading_clients_count != 0:

      influx_service={}
      influx_service['image']='influxdb'
      influx_service['hostname']='influxdb'
      influx_service['volumes'] = ['influxdb:/var/lib/influxdb2', 'influxdb_etc:/etc/influxdb2']
      influx_service['ports'] = ['8086:8086']
      influx_service['environment'] = [
         'DOCKER_INFLUXDB_INIT_USERNAME=admin',\
         'DOCKER_INFLUXDB_INIT_PASSWORD=password',\
         'DOCKER_INFLUXDB_INIT_ORG=trading_client',\
         'DOCKER_INFLUXDB_INIT_BUCKET=trading_client',\
         'DOCKER_INFLUXDB_INIT_MODE=setup'\
      ]

      volumes['influxdb']=None
      volumes['influxdb_etc']=None
      services['influxdb']=influx_service

   # add trading clients
   for index in range(1, trading_clients_count+1):
      name = 'trading_client_' + configs['environment'] + '_' + str(index)
      client_service = {}
      client_service['container_name'] = name
      client_service['image'] = configs['trading_client_image']

      # check that volume dir already exists
      client_keys_path = os.path.abspath(os.path.join(configs['environment'], name))
      if not os.path.isdir(client_keys_path):
         print(f'ERROR: keys dir does not exists: {client_keys_path}')
         exit(1)

      client_service['volumes'] = [f'{client_keys_path}:/usr/app/keys']
      client_service['environment'] = [
         'PYTHONUNBUFFERED=1',\
         'DB_HOST=http://influxdb:8086',\
         'DB_ORG=trading_client',\
         'DB_BUCKET=trading_client',\
         'ORDER_SEND_INTERVAL={}'.format(configs['order_send_interval']),\
         'DB_TOKEN={}'.format(configs['db_token']),\
         'API_ENDPOINT={}'.format(configs['api_endpoint']),\
         'LOGIN_SERVICE_ENDPOINT={}'.format(configs['login_endpoint']),\
         'ENV_NAME={}'.format(configs['environment']),\
         'TARGET_PRODUCT={}'.format(configs['target_product']),\
         'MIN_CASH_AMOUNT={}'.format(min_cash_amount)\

      ]
      client_service['restart']='on-failure'
      services[name]=client_service

   # add dealing clients
   for index in range(1, int(configs['dealing_clients'])+1):
      name = 'dealing_client_' + configs['environment'] + '_' + str(index)
      dealer_service = {}
      dealer_service['container_name'] = name
      dealer_service['image'] = configs['dealing_client_image']
      dealer_keys_path = os.path.abspath(os.path.join(configs['environment'], name))
      if not os.path.isdir(dealer_keys_path):
         print(f'ERROR: keys dir does not exists: {dealer_keys_path}')
         exit(1)

      dealer_service['volumes'] = [f'{dealer_keys_path}:/usr/app/keys']
      dealer_service['environment'] = [
         'PYTHONUNBUFFERED=1',\
         'API_ENDPOINT={}'.format(configs['api_endpoint']),\
         'LOGIN_SERVICE_ENDPOINT={}'.format(configs['login_endpoint']),\
         'TARGET_PRODUCT={}'.format(configs['target_product']),\
         'MIN_CASH_AMOUNT={}'.format(min_cash_amount)\
      ]
      dealer_service['restart']='on-failure'
      services[name]=dealer_service

   if len(volumes) != 0:
      compose_file['volumes']=volumes
   compose_file['services']=services
   with open('docker-compose.yml', 'w') as file_stream:
      yaml.dump(compose_file, file_stream)

if __name__ == "__main__":
   input_parser = argparse.ArgumentParser()

   input_parser.add_argument('--config',
                             help='Path to file for suite generation',
                             required=True,
                             action='store')

   args = input_parser.parse_args()
   generate_suite(configs_path=args.config)
   exit(0)

