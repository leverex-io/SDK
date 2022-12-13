from Factories.StatusReporter.Factory import Factory, MAKER, TAKER
from Factories.Definitions import Position, Balance, Ready

import websockets, asyncio, traceback, logging, json
import datetime
import ssl

class DataProxyObject:
   def __init__(self):
      self.ready_state = None
      self.balances = None
      self.positions = None

class WebReporter(Factory):
   def __init__(self, config):
      self._connection = None
      self._buffer = []
      super().__init__(config)

   async def connect(self):
      ssl_context = ssl.SSLContext(ssl.PROTOCOL_TLS_CLIENT)
      ssl_context.load_verify_locations(self.config["exporter_service"]["client_cert"]))
      async with websockets.connect(self.config["exporter_service"]["url"], ssl=ssl_context) as websocket:
        self._connection = websocket
        while True:
            try:
                message = await self._connection.recv()
                logging.info(message)

            except websockets.exceptions.ConnectionClosed:
                print('ConnectionClosed')
                break
      
   
   async def sendMessage(self, data):
      if self._connection:
         try:
            obj = data or self.createDataProxy()
            await self._connection.send(json.dumps(obj.__dict__))
         except Exception as e:
            logging.error("failed to send message: {}".format(traceback.format_exec()))

   def createDataProxy(self):
       obj = DataProxyObject()
       obj.ready_state = self.ready_state
       obj.balances = self.balances
       obj.positions = self.positions
       return obj

   async def flushBuffer(self):
      for data in self._buffer:
         await self.sendMessage(data)

   def getAsyncIOTask(self):
      return asyncio.create_task(self.connect())

   async def report(self, __):
      if not self._connection:
         self._buffer.append(self.createDataProxy())
      await self.flushBuffer()
      await self.sendMessage(self.__dict__)
