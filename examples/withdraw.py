import argparse
import asyncio
import logging
import sys

sys.path.append('..')

from trader_core.api_connection import AsyncApiConnection


class WithdrawMaker():
   def __init__(self, key_file, withdraw_amount, withdraw_address):
      self._withdraw_amount = withdraw_amount
      self._withdraw_address = withdraw_address

      self._leverex_connection = AsyncApiConnection(customer_email='test@email.com',
                                                    api_endpoint='wss://api-dev.leverex.io',
                                                    login_endpoint='wss://login-dev.leverex.io/ws/v1/websocket',
                                                    key_file_path=key_file)

   def run(self):
      logging.info('Starting leverex connection')
      asyncio.run(self._leverex_connection.run(self))

   async def on_withdraw_request_response(self, withdraw_info):
      print(f'Create withdraw response: {str(withdraw_info)}')

   async def on_withdraw_update(self, withdraw_info):
      logging.info(f'Withdraw update: {str(withdraw_info)}')

   async def on_authorized(self):
      logging.info('Authorized to leverex')
      logging.info(f'Creating withdraw for {self._withdraw_amount}')

      await self._leverex_connection.withdraw_liquid(address=self._withdraw_address,
                                                     currency='USDT',
                                                     amount=self._withdraw_amount,
                                                     callback=None)

   async def updateOffer(self):
      pass


def main(key_file, withdraw_amount, withdraw_address):
   dealer = WithdrawMaker(key_file, int(withdraw_amount), withdraw_address)
   asyncio.run(dealer.run())


if __name__ == '__main__':
   input_parser = argparse.ArgumentParser()

   input_parser.add_argument('--key_file',
                             help='Login key file',
                             action='store',
                             required=True)

   input_parser.add_argument('--withdraw_amount',
                             help='USDT amount to withdraw',
                             action='store',
                             required=True)

   input_parser.add_argument('--withdraw_address',
                             help='Address to withdraw to',
                             action='store',
                             required=True)

   args = input_parser.parse_args()

   logging.basicConfig(level=logging.INFO)
   logging.getLogger("asyncio").setLevel(logging.INFO)

   main(key_file=args.key_file, withdraw_amount=args.withdraw_amount, withdraw_address=args.withdraw_address)

   exit(0)
