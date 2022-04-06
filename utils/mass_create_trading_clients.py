import argparse
import os
import subprocess
import shutil

def create_customers(env='dev',total_count=100):
   env_dir = os.path.join(os.getcwd(), f'{env}')

   prefix=f'trading_client_{env}_'
   created_count = 0

   for index in range(1, total_count+1):
      client_name = f'{prefix}{index}'
      client_path = os.path.join(env_dir, client_name)
      if os.path.exists(client_path):
         print(f'{client_name} already exists')
      else:
         print(f'Creating {client_name} at {client_path}')
         command = ['./generate_keys.sh'\
                   ,env\
                   ,client_name\
                   ,'+'+str(380561240000+index)]
         result = subprocess.run(command, stdout=subprocess.PIPE)
         if result.returncode != 0:
            print(f'ERROR: failed to create {client_name}')
            shutil.rmtree(client_path)
            exit(1)
         print('Client created successfully')
         created_count=created_count+1

   print(f'{total_count} rtading clients available. {created_count} were created in this run')

if __name__ == "__main__":
   input_parser = argparse.ArgumentParser()

   input_parser.add_argument('--env',
                             help='Target env',
                             required=True,
                             action='store')

   input_parser.add_argument('--total_count',
                             help='Count of trading clients that should exists',
                             required=True,
                             type=int,
                             action='store')

   args = input_parser.parse_args()
   create_customers(env=args.env, total_count=args.total_count)
   exit(0)
