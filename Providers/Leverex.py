import logging
import asyncio
import json
import time
from decimal import Decimal

from Factories.Provider.Factory import Factory
from Factories.Definitions import PositionsReport, \
   BalanceReport, PriceEvent, \
   CashOperation, TheTxTracker, \
   checkConfig

from leverex_core.utils import WithdrawInfo, LeverexOpenVolume, \
   round_down
from leverex_core.base_client import LeverexBaseClient

################################################################################
class LeverexPositionsReport(PositionsReport):
   def __init__(self, provider):
      super().__init__(provider)

      #get sessionId for current session
      sessionId = None
      if provider.currentSession != None:
         sessionId = provider.currentSession.getSessionId()

      #grab orders for session id
      self.orderData = None
      if sessionId in provider.orderData:
         self.orderData = provider.orderData[sessionId]

         #set index price for orders, it will update pnl
         self.orderData.setIndexPrice(self.indexPrice)

   def __str__(self):
      #header
      pnl = self.getPnl()
      result = " ** {} -- exp: {}".format(\
         self.name, self.netExposure)

      #grab session from orderData
      session = None
      if self.orderData != None:
         session = self.orderData.session

      #print session info
      if session is not None and session.isOpen():
         result += " -- session: {}, open price: {}".format(
            session.getSessionId(), session.getOpenPrice())
      result += "\n"

      if self.getOrderCount() == 0:
         result += "    N/A\n"
         return result

      #positions
      orderDict = {
         'ROLL' : [],
         'MAKER' : [],
         'TAKER' : []
      }

      if self.orderData != None:
         for pos in self.orderData.orders:
            order = self.orderData.orders[pos]
            if order.is_trade_position():
               if order.is_taker:
                  orderDict['TAKER'].append(pos)
               else:
                  orderDict['MAKER'].append(pos)
            else:
               orderDict['ROLL'].append(pos)

      for posType in orderDict:
         orderList = orderDict[posType]
         if len(orderList) == 0:
            continue
         result += " *  - {} -\n".format(posType)

         for i in range(0, len(orderList)):
            orderId = orderList[i]
            result += " *    {}".format(str(self.orderData.orders[orderId]))
            if i < len(orderList) - 1:
               result += "\n"

         if posType is not next(reversed(orderDict.keys())):
            result += "\n"

      return result

   def __eq__(self, obj):
      if not super().__eq__(obj):
         return False
      return self.orderData == obj.orderData

   def getOrderCount(self):
      if self.orderData == None:
         return 0
      return self.orderData.getCount()

   def getPnl(self):
      if self.orderData == None:
         return "N/A"

      pnl = 0
      for orderId in self.orderData.orders:
         orderPL = self.orderData.orders[orderId].trade_pnl
         if orderPL == None:
            return "N/A"
         pnl += orderPL
      return round_down(pnl, 6)

################################################################################
class LeverexBalanceReport(BalanceReport):
   def __init__(self, provider):
      super().__init__(provider)
      self.balances = provider.balances
      self.ccy = provider.ccy

   def __str__(self):
      #header
      result = " +- {}:\n".format(self.name)

      #breakdown
      for ccy in self.balances:
         result += " +  <{}: {}".format(ccy, round_down(self.balances[ccy], 2))
         if ccy == self.ccy:
            result += " (total)"
         result += ">\n"

      if len(self.balances) == 0:
         result += " +  <N/A>\n"

      return result

   def __eq__(self, obj):
      if not super().__eq__(obj):
         return False

      if self.balances.keys() != obj.balances.keys():
         return False

      for ccy in self.balances:
         if self.balances[ccy] == None:
            return False

         if self.balances[ccy] != obj.balances[ccy]:
            return False

      return True

################################################################################
class LeverexWithdrawal(CashOperation):
   def __init__(self, amount, callback):
      super().__init__()
      self.amount = amount
      self.withdrawalId = None
      self.callback = callback

   def setup(self, leverex):
      return True

   async def doTheTask(self, leverex):
      async def withdrawCallback(withdrawal):
         self.withdrawalId = withdrawal.id
         leverex.withdrawalHistory[withdrawal.id] = withdrawal
         if self.callback != None:
            await self.callback()

      await leverex.connection.withdraw_liquid(
         address=leverex.chainAddresses.getDefaultWithdrawAddr(),
         currency=leverex.ccy,
         amount=self.amount,
         callback=withdrawCallback
      )
      return True

   def assessProgress(self, leverex):
      if self.withdrawalId not in leverex.withdrawalHistory:
         return False

      wtdState = leverex.withdrawalHistory[self.withdrawalId].status_code
      return wtdState == WithdrawInfo.WITHDRAW_COMPLETED

   def __str__(self):
      result = "#{} Withdrawal, stage: {}\n".format(
         self.id(), self.stageStr())
      result += " |    - amount: {}, id: {}\n".format(
         self.amount, self.withdrawalId)
      return result

   def __eq__(self, other):
      if not isinstance(other, LeverexWithdrawal):
         return False
      return self.amount == other.amount

########
class LeverexCancelWithdrawal(CashOperation):
   def __init__(self):
      super().__init__()
      self.ids = []

   def setup(self, leverex):
      #the list of ids is used to check for the completion condition
      #so set the list first, then start cancelling withdrawals
      for wId in leverex.withdrawalHistory:
         if not leverex.withdrawalHistory[wId].canBeCancelled():
            continue
         self.ids.append(wId)

      #if there are no withdrawals, we are done
      if not self.ids:
         return None
      return True

   async def doTheTask(self, leverex):
      for wId in self.ids:
         async def callback(withdraw_info):
            #TODO: handle failures to cancel
            leverex.withdrawalHistory[withdraw_info.id] = withdraw_info
            #cancelled withdrawal replies come along balance notifications
            #there is no need to fire a position notification here
         await leverex.connection.cancel_withdraw(id=wId, callback=callback)
      return True

   def assessProgress(self, leverex):
      completed = True
      for wId in self.ids:
         if wId not in leverex.withdrawalHistory:
            return False

         wtdState = leverex.withdrawalHistory[wId].status_code
         if wtdState != WithdrawInfo.WITHDRAW_CANCELLED:
            completed = False
            break

      return completed

   def __str__(self):
      result = "#{} Cancellation, stage: {}\n".format(
         self.id, self.stageStr())
      for id in self.ids:
         result += " |    - id: {}\n".format(id)
      return result

   def __eq__(self, other):
      if not isinstance(other, LeverexCancelWithdrawal):
         return False
      return self.ids == other.ids

################################################################################
class OfferToPush(object):
   def __init__(self, offers):
      self.offers = offers
      self.timestamp = self.getTimeMs() #time in milliseconds
      self.waiting = False

   def getTimeMs(self):
      return time.time_ns() / 1000000

   def updateOffers(self, offers):
      #update offers but not the timestamp
      self.offers = offers

   def updateTimestamp(self):
      self.timestamp = self.getTimeMs()

   def ready(self):
      if self.waiting:
         return False
      return self.getTimeMs() >= self.timestamp + 200

   async def wait(self):
      if self.waiting:
         return
      self.waiting = True
      diff = self.timestamp + 200 - self.getTimeMs()
      if diff <= 0:
         return
      await asyncio.sleep(diff/1000)
      self.waiting = False

################################################################################
class LeverexProvider(Factory, LeverexBaseClient):
   required_settings = {
      'leverex': [
         'key_file_path'
      ]
   }

   ## setup ##
   def __init__(self, config):
      LeverexBaseClient.__init__(self, config)
      Factory.__init__(self, "Leverex")

      self.lastReadyState = False
      self.offerToPush = None

      #check for required config entries
      checkConfig(self.config, self.required_settings)

      #leverex leverage is locked at 10x
      self.setLeverage(10)

   ##
   def setup(self, callback):
      Factory.setup(self, callback)
      LeverexBaseClient.setupConnection(self)

   ##
   def getAsyncIOTask(self):
      return asyncio.create_task(self.connection.run(self))

   #############################################################################
   #### withdrawals
   #############################################################################
   async def loadWithdrawals(self, callback):
      async def wtdrCallback(withdrawals):
         self.withdrawalHistory = {}
         for wtd in withdrawals:
            self.withdrawalHistory[wtd.id] = wtd
         await callback()
      await self.connection.load_withdrawals_history(wtdrCallback)

   ##
   def withdrawalsLoaded(self):
      return self.withdrawalHistory is not None

   ##
   async def on_withdraw_update(self, withdrawal):
      self.withdrawalHistory[withdrawal.id] = withdrawal
      await self.onBalanceUpdate()

   ##
   async def withdraw(self, amount, callback):
      task = self.cashOps.addTask(LeverexWithdrawal(amount, callback))
      await self.cashOps.process()
      return task

   ##
   async def cancelWithdrawals(self):
      task = self.cashOps.addTask(LeverexCancelWithdrawal())
      await self.cashOps.process()
      return task

   ##
   def getPendingWithdrawals(self):
      wtdList = []
      for wId in self.withdrawalHistory:
         if self.withdrawalHistory[wId].isPending():
            wtdList.append(self.withdrawalHistory[wId])
      return wtdList

   #############################################################################
   #### events
   #############################################################################

   ## connection status events ##
   def on_connected(self):
      pass

   async def on_authorized(self):
      await Factory.setConnected(self, True)
      await self.subscribe()

   ## balance events ##
   async def on_balance_update(self, balances):
      if not self.balanceInitialized():
         await self.setInitBalance()
         await self.evaluateReadyState()

      await LeverexBaseClient.on_balance_update(self, balances)
      await self.onBalanceUpdate()

   ## position events ##
   async def on_positions_loaded(self, orders):
      await LeverexBaseClient.on_positions_loaded(self, orders)
      await Factory.setInitPosition(self)
      await self.evaluateReadyState()

   async def on_order_event(self, order, eventType):
      if self.storeOrder(order, eventType):
         await Factory.onPositionUpdate(self)

   ## session notifications ##

   async def setSession(self, session):
      await LeverexBaseClient.setSession(self, session)
      await self.evaluateReadyState()

      #notify on new open price
      await self.setOpenPrice(self.currentSession.getOpenPrice())

   ## index price ##
   async def on_market_data(self, marketData):
      await LeverexBaseClient.on_market_data(self, marketData)
      await self.dealerCallback(self, PriceEvent)

   ## deposits ##
   async def on_deposit_update(self, deposit_info):
      TheTxTracker.addDeposit(deposit_info)
      self.onTransactionEvent()

   #############################################################################
   #### methods
   #############################################################################

   ## state ##
   def isReady(self):
      return self.lastReadyState

   def isBroken(self):
      if self.currentSession == None:
         return False
      return not self.currentSession.isHealthy()

   def getStatusStr(self):
      if not Factory.isReady(self):
         return Factory.getStatusStr(self)

      if self.currentSession == None:
         return "missing session data"
      if not self.currentSession.isOpen():
         return "session is closed"
      if not self.currentSession.isHealthy():
         return "session is damaged"

      return "N/A"

   async def getInitialData(self):
      await LeverexBaseClient.subscribeToInitialData(self)

   async def evaluateReadyState(self):
      def assessFactoryState():
         if not Factory.isReady(self):
            return False
         return True

      def assessSessionState():
         if self.currentSession == None or \
            not self.currentSession.isOpen() or \
            not self.currentSession.isHealthy():
            return False
         return True

      #assess current state
      factoryState = assessFactoryState()
      sessionState = assessSessionState()
      currentReadyState = factoryState and sessionState

      #has state changed?
      if self.lastReadyState == currentReadyState:
         if currentReadyState == True:
            #provider is ready, nothing to do
            return

         '''
         provider isn't ready but session state may have changed,
         we need to check for this and initialize state accordingly
         '''
      else:
         self.lastReadyState = currentReadyState

      if currentReadyState == False:
         #provider isn't ready
         if sessionState == False:
            #session isn't ready, reset init flags
            self.resetInitFlags()
         else:
            #session is ready, get initial data
            await self.fetchInitialData()

      await Factory.onReady(self)

   ## offers ##
   def getOpenVolume(self):
      if not self.isReady():
         return None

      if self.currentSession == None:
         return None

      try:
         return LeverexOpenVolume(self)
      except:
         return None

   def getCashMetrics(self):
      if self.ccy not in self.balances:
         return None
      balance = self.balances[self.ccy]
      if self.margin_ccy in self.balances:
         balance += self.balances[self.margin_ccy]

      pending = 0
      if self.withdrawalsLoaded():
         for wId in self.withdrawalHistory:
            withdrawal = self.withdrawalHistory[wId]
            if withdrawal.isPending():
               pending += float(withdrawal.amount)

      return {
         'total' : Decimal(balance),
         'pending' : Decimal(pending),
         'ratio' : self.getCollateralRatio(),
         'price' : self.currentSession.getOpenPrice()
      }

   async def submitOffers(self, offers):
      def callback(reply):
         if 'submit_offer' not in reply:
            return
         if 'result' not in reply['submit_offer']:
            return

         if reply['submit_offer']['result'] != 1:
            logging.error(f"Failed to submit offers with error: {str(reply)}")

      #do not push offers more than once every 200ms
      if self.offerToPush != None:
         if (self.offerToPush.ready()):
            self.offerToPush = None
         else:
            self.offerToPush.updateOffers(offers)
            await self.offerToPush.wait()
            if not self.offerToPush.ready():
               return

      if self.offerToPush == None:
         self.offerToPush = OfferToPush(offers)
      self.offerToPush.updateTimestamp()

      await self.connection.submit_offers(
         target_product=self.product,
         offers=self.offerToPush.offers,
         callback=callback)

   ## getters ##
   def getPositions(self):
      return LeverexPositionsReport(self)

   def getBalance(self):
      return LeverexBalanceReport(self)

   def getExposure(self):
      if not self.isReady():
         return None
      return LeverexBaseClient.getExposure(self)
