import requests
import json
import datetime
import websockets
import random
import logging
import pyqrcode
import platform
import io

from jwcrypto import jwk, jws, jwe
from jwcrypto.common import json_encode

from PIL import Image, ImageDraw

####
class LoginException(Exception):
   def __init__(self, errStr):
      super().__init__(errStr)

####
class LoginServiceClientWS():
   def __init__(self,
      private_key_path,
      login_endpoint,
      email=None,
      dump_communication=False,
      aeid_endpoint=None,
      service_url=None
      ):

      self._dump_communication = dump_communication
      self._login_endpoint = login_endpoint
      self._email = email
      self._messages = {}
      self._service_url = None

      if private_key_path is not None:
         with open(private_key_path, 'r') as key_file:
            self._key = jwk.JWK()
            self._key.import_from_pem(key_file.read().encode())
      else:
         self._key = None
      self._aeid_endpoint = aeid_endpoint

   def get_email(self):
      if self._email == None:
         raise Exception("missing email")
      return self._email

   def get_service_url(self):
      if self._service_url == None:
         raise Exception("missing service_url")
      return self._service_url

   def get_login_endpoint(self):
      return self._login_endpoint


   async def send_key_to_endpoint(self):
      #print out the key fingerprint
      logging.info("Uploading key {} to login server for account:{}".format(
         self._key.thumbprint(), self._email))

      #randomize messageId
      messageId = random.randint(0, 2**32-1)

      #create the upload_key_init packet
      data = {'method': "upload_key_init", 'api': "login",
         'args': {
            'email': self.get_email(),
            'service_url': self.get_service_url(),
            'user_cert': self._key.export_public(True)
            },
         #randomize the message id, it will be sent back to us on reply
         'message_id': messageId}

      #send to login server
      logging.info("Connecting to login endpoint: {}".format(self._login_endpoint))
      async with websockets.connect(self._login_endpoint) as websocket:
         await websocket.send(json.dumps(data))

         logging.info('Request sent. Please approve on your eID device.')
         #await response, this should prompt the autheid account
         #attached to this email for vetting the key
         while True:
            resp = await websocket.recv()

            #report status, we're done
            uploadResult = json.loads(resp)

            if uploadResult['error'] is not None:
               error_message = uploadResult['error']
               logging.error(f'Request failed: {error_message}')
               return False

            if uploadResult['message_id'] != messageId:
               logging.warning(f'Unexpected reply: {uploadResult}')
               continue
            else:
               if not 'data' in uploadResult:
                  continue

               operation_status = uploadResult['data']['status']
               if operation_status == 'PENDING':
                  # waiting for user action
                  continue
               elif operation_status == 'USER_CANCELLED':
                  logging.error('Request rejected')
                  return False
               elif operation_status == 'TIMEOUT':
                  logging.error('Request sign timeout')
                  return False
               elif operation_status == 'SUCCESS':
                  logging.info('Request accepted')
                  return True
               else:
                  logging.error(f'Error: unexpected status {operation_status}')
                  return False

      return False

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
   # {
   #    'access_token': 'token string'
   #    , 'grant': 'basic'
   #    , 'expires_in': 600
   # }
   async def logMeIn(self, api_enpoint_url):
      if self._key != None:
         return await self.get_access_token_from_key(api_enpoint_url)
      elif self._aeid_endpoint:
         return await self.get_access_token_from_request(api_enpoint_url)
      else:
         raise LoginException("invalid setup, cannot login!")

   ## generate and sign access token provided private key
   async def get_access_token_from_key(self, api_enpoint_url):
      token_dict = {
           'thumbprint': self._key.thumbprint(),
           'created': '{}'.format(datetime.datetime.utcnow()),
           'service_url': api_enpoint_url
      }
      token = json.dumps(token_dict)
      serialized_token = self._sign_token(token)

      messageId = random.randint(0, 2**32-1)
      data = {'method': "new", 'api': "login",
         'args': {
            'signed_challenge': serialized_token,
            },
         #randomize the message id, it will be sent back to us on reply
         'message_id': messageId}

      async with websockets.connect(self._login_endpoint) as websocket:
         if self._dump_communication:
            logging.info('Sending {}'.format(json.dumps(data)))
         await websocket.send(json.dumps(data))

         while True:
            resp = await websocket.recv()

            #report status, we're done
            uploadResult = json.loads(resp)

            if self._dump_communication:
               logging.info('Received {}'.format(json.dumps(uploadResult)))

            if uploadResult['message_id'] != messageId:
               logging.warning("Unexpected response: {}".format(uploadResult))
               continue
            else:
               break

         return uploadResult['data']

   ## get request id to generate token from 2FA
   async def get_access_token_from_request(self, api_enpoint_url):
      messageId = random.randint(0, 2**32-1)
      data = {'method': "login_init",
         'api': "login",
         'args': {'service_url': api_enpoint_url},
         #randomize the message id, it will be sent back to us on reply
         'message_id': messageId}

      async with websockets.connect(self._login_endpoint) as websocket:
         if self._dump_communication:
            logging.info('Sending {}'.format(json.dumps(data)))
         await websocket.send(json.dumps(data))

         while True:
            resp = await websocket.recv()
            loginReply = json.loads(resp)

            if self._dump_communication:
               logging.info('Received {}'.format(resp))

            if loginReply['message_id'] != messageId:
               logging.warning("Unexpected login reply: {}".format(resp))
               continue

            # handle login server replies
            if loginReply['method'] == 'login_init':
               requestId = loginReply['data']['request_id']
               requestUrl = f"{self._aeid_endpoint}/app/requests/?request_id={requestId}"
               qr = pyqrcode.create(requestUrl)

               #display login QR
               print ("login request has been created, scan this QR with your aeid mobile app to proceed")

               if platform.system() == 'Windows':
                   buffer = io.BytesIO()
                   qr.png(buffer, scale=10)
                   buffer.seek(0)
                   img = Image.open(buffer)
                   img.show(title="Scan this QR with your Autheid mobile App")
               else:
                   print(qr.terminal())

               #wait on request status update
               continue

            elif loginReply['method'] == 'login_status':
               try:
                  statusBody = loginReply['data']
                  if 'status' in statusBody:
                     print(f"login status update: {statusBody['status']}")
                     continue
                  elif 'error' in statusBody:
                     print(f"login error: {statusBody['error']}")
                     raise LoginException(f"Login attempt failed with error: {statusBody['error']}")
               except:
                  logging.warning("failed to parse login status update: {}".format(resp))

            elif loginReply['method'] == 'login_complete':
               replyBody = loginReply['data']
               if replyBody['status'] != 'SUCCESS':
                  raise LoginException(f"Login attempt failed with status: {replyBody['status']}")
               else:
                  return replyBody

            else:
               logging.warning("Unhandled login reply: {}".format(resp))
               continue

   ## refresh session JWT
   async def update_access_token(self, access_token):
      messageId = random.randint(0, 2**32-1)
      data = {'method': "renew", 'api': "login",
         'args': {
            'access_token': access_token,
            },
         #randomize the message id, it will be sent back to us on reply
         'message_id': messageId}

      async with websockets.connect(self._login_endpoint) as websocket:
         if self._dump_communication:
            logging.info('Sending {}'.format(json.dumps(data)))
         await websocket.send(json.dumps(data))

         while True:
            resp = await websocket.recv()

            #report status, we're done
            uploadResult = json.loads(resp)

            if self._dump_communication:
               logging.info('Received {}'.format(json.dumps(uploadResult)))

            if uploadResult['message_id'] != messageId:
               logging.warning("Unexpected reply: {}".format(uploadResult))
               continue
            else:
               break

         return uploadResult['data']
