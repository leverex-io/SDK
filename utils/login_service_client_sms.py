import requests
import json
import datetime
import websockets
import random
import logging

from jwcrypto import jwk, jws, jwe
from jwcrypto.common import json_encode

class LoginServiceClient():
   def __init__(self, phone_number, private_key_path, login_endpoint, dump_communication=False):
      self._dump_communication = dump_communication
      self._login_endpoint = login_endpoint
      self._phone_number = phone_number

      with open(private_key_path, 'r') as key_file:
         self._key = jwk.JWK()
         self._key.import_from_pem(key_file.read().encode())

   def get_phone(self):
      return self._phone_number

   def get_login_endpoint(self):
      return self._login_endpoint

   def _send_request(self, endpoint, headers, data):
      url = '{}/{}'.format(self.get_login_endpoint(), endpoint)

      if self._dump_communication:
         print(f'sending to {url}\nheaders: {headers}\n  data:{data}')
      response = requests.post(url, data=json.dumps(data), headers=headers)

      response.raise_for_status()
      result = response.json()

      if self._dump_communication:
         print(f'Response: {result}')

      return result

   def send_key_to_endpoint(self):
      default_headers = {'Content-Type': 'application/json'}
      data = {'phone': self.get_phone(), 'public_key': self._key}

      result = self._send_request(endpoint='api/v1/signup', headers=default_headers, data=data)


   def confirm_key_submit(self, sms_code):
      serialized_token = self._sign_token(sms_code)

      default_headers = {'Content-Type': 'application/json'}

      body = {'signed_code': serialized_token}

      result = self._send_request(endpoint='api/v1/signup/confirm', headers=default_headers, data=body)
      return result['status']

   def _sign_token(self, token):
      jws_token = jws.JWS(token.encode('utf-8'))
      header = {
         'kid': self._key.thumbprint(),
      }
      jws_token.add_signature(
         self._key, None, json_encode({'alg': 'ES256'}), json_encode(header)
      )

      return jws_token.serialize(compact=False)

   # get_access_token return
   #  {
   #    'access_token': 'token string'
   #    , 'grant': 'basic'
   #    , 'expires_in': 600
   # }
   # it is not really async, just to sustain interface
   async def get_access_token(self, api_enpoint_url):
      data = {
           'thumbprint': self._key.thumbprint(),
           'created': '{}'.format(datetime.datetime.utcnow()),
           'service_url': api_enpoint_url
      }
      token = json.dumps(data)
      serialized_token = self._sign_token(token)

      default_headers = {'Content-Type': 'application/json'}

      body = {
         'signed_challenge': serialized_token,
      }

      return self._send_request(endpoint='api/v1/token', headers=default_headers, data=body)

   # it is not really async, just to sustain interface
   async def update_access_token(self, access_token):
      default_headers = {'Content-Type': 'application/json'}

      body = {
         'access_token': access_token,
      }

      return self._send_request(endpoint='api/v1/session', headers=default_headers, data=body)
