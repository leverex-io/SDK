import asyncio
import logging

from Factories.Hedger.Factory import HedgerFactory
from Factories.Definitions import PriceOffer, OfferException, \
   Rebalance, RebalanceReport

CANCEL_PENDING          = 'cancel_pending'
CANCEL_PENDING_TODO     = 1
CANCEL_PENDING_ONGOING  = 2
CANCEL_PENDING_DONE     = 3

WITHDRAW             = 'withdraw'
WITHDRAW_TODO        = 4
WITHDRAW_ONGOING     = 5
WITHDRAW_DONE        = 6

################################################################################
class HedgerException(Exception):
   pass

################################################################################
## Rebalance
################################################################################
class ProviderTarget(object):
   def __init__(self, provider, target):
      self.provider = provider
      self.cash = provider.getCashMetrics()
      self.target = target

      '''
      Compute the amount to move across providers. If there
      are pending withdrawals, check whether we should cancel
      them first
      '''

      self.cancelPending = CANCEL_PENDING_DONE
      self.toWithdraw = {
         'status' : WITHDRAW_DONE,
         'amount' : 0
      }

      if self.cash['total'] + self.cash['pending'] > self.target:
         #provider has more cash than it needs
         self.toWithdraw['status'] = WITHDRAW_TODO
         self.toWithdraw['amount'] = self.cash['total'] - self.target

         if self.cash['pending'] > 0:
            #avoid multiple withdrawals when possible
            self.cancelPending = CANCEL_PENDING_TODO
            self.toWithdraw['amount'] += self.cash['pending']

      elif self.cash['total'] < self.target:
         #provider has less cash than desired
         if self.cash['pending'] != 0:
            #provider has pending withdrawals, cancel them
            self.cancelPending = CANCEL_PENDING_TODO

      #8 decimals max
      self.toWithdraw['amount'] = round(self.toWithdraw['amount'], 8)

   def getCashMetrics(self):
      return self.provider.getCashMetrics()

####
class RebalanceTarget(object):
   STATE_INIT              = 1
   STATE_NO_REBALANCE      = 2
   STATE_CANCELLING_WTDR   = 3
   STATE_WITHDRAWING       = 4
   STATE_COMPLETED         = 5

   def __init__(self, config, maker, makerTarget, taker, takerTarget):
      self.state = self.STATE_INIT
      self.min_amount = float(config['rebalance']['min_amount'])
      self.threshold = float(config['rebalance']['threshold_pct'])

      self.maker = ProviderTarget(maker, makerTarget)
      self.taker = ProviderTarget(taker, takerTarget)

   def needsRebalance(self):
      if self.maker.cancelPending != CANCEL_PENDING_DONE or \
         self.taker.cancelPending != CANCEL_PENDING_DONE:
         return True

      amount = max(\
         self.maker.toWithdraw['amount'], \
         self.taker.toWithdraw['amount'])
      if amount < self.min_amount:
         return False

      allCash = self.maker.cash['total'] + self.maker.cash['pending'] + \
         self.taker.cash['total'] + self.taker.cash['pending']
      return (amount / allCash) >= self.threshold

   def inTransit(self):
      return self.state != self.STATE_NO_REBALANCE and \
         self.state != self.STATE_COMPLETED

   ##
   def evaluateCancellations(self):
      result = {}

      ## maker ##
      def evaluate(provider):
         resultInner = None
         if provider.cancelPending == CANCEL_PENDING_TODO:
            #mark maker withdrawals for cancellation
            resultInner = {CANCEL_PENDING : True}
            provider.cancelPending = CANCEL_PENDING_ONGOING

         elif provider.cancelPending == CANCEL_PENDING_ONGOING:
            #check if maker pending withdrawals were cancelled
            cashMetrics = provider.getCashMetrics()
            if cashMetrics['pending'] == 0:
               #mark maker withdrawals as cancelled
               provider.cancelPending = CANCEL_PENDING_DONE

         return resultInner

      result['maker'] = evaluate(self.maker)
      result['taker'] = evaluate(self.taker)

      ## end condition ##
      if self.maker.cancelPending == CANCEL_PENDING_DONE and \
         self.taker.cancelPending == CANCEL_PENDING_DONE:
         return None, True

      return result, False

   ##
   def evaluateWithdrawals(self):
      result = {}

      def evaluate(provider):
         resultInner = None
         if provider.toWithdraw['status'] == WITHDRAW_TODO:
            #mark maker for withdrawal
            resultInner = { WITHDRAW : provider.toWithdraw['amount'] }
            provider.toWithdraw['status'] = WITHDRAW_ONGOING

         elif provider.toWithdraw['status'] == WITHDRAW_ONGOING:
            cashMetrics = provider.getCashMetrics()
            if cashMetrics['total'] >= provider.target:
               #maker balance has met target
               provider.toWithdraw['status'] = WITHDRAW_DONE

         return resultInner

      result['maker'] = evaluate(self.maker)
      result['taker'] = evaluate(self.taker)

      if self.maker.getCashMetrics()['total'] >= self.maker.target and \
         self.taker.getCashMetrics()['total'] >= self.taker.target:
         return None, True

      return result, False

   ##
   def progress(self):
      if self.state == self.STATE_INIT:
         #is there a rebalance to perform?
         if not self.needsRebalance():
            self.state = self.STATE_NO_REBALANCE
            return None
         self.state = self.STATE_CANCELLING_WTDR
         return self.progress()

      elif self.state == self.STATE_CANCELLING_WTDR:
         #deal with withdrawal cancellations
         result, done = self.evaluateCancellations()

         if result != None:
            return result
         elif done == True:
            self.state = self.STATE_WITHDRAWING
            return self.progress()
         else:
            return None

      elif self.state == self.STATE_WITHDRAWING:
         #create new withdrawals
         result, done = self.evaluateWithdrawals()

         if result != None:
            return result
         elif done == True:
            self.state = self.STATE_COMPLETED
         return None

      return None

################################################################################
class RebalanceManager(object):
   LOAD_ADDRESS_PENDING    = 1
   LOAD_ADDRESS_MISMATCH   = 2
   LOAD_ADDRESS_DONE       = 3

   ## setup ##
   def __init__(self, config, maker, taker, onEventFunc):
      self.maker = maker
      self.taker = taker
      self.target = None
      self.onEventFunc = onEventFunc

      self.config = config
      self.enabled = config['rebalance']['enable']

      self.loadedWithdrawals = False
      self.loadedAddresses = self.LOAD_ADDRESS_PENDING

   def canAssess(self):
      #do not assess rebalance if we are requesting withdrawals
      if self.target != None and self.target.inTransit():
         return False
      return self.loadedWithdrawals

   def canWithdraw(self):
      return self.loadedWithdrawals and self.enabled and \
         self.loadedAddresses == self.LOAD_ADDRESS_DONE

   def needsRebalance(self):
      if self.target == None:
         return False
      return self.target.needsRebalance()

   ##
   async def completeSetup(self):
      if self.maker.chainAddresses.hasDepositAddr():
         #NOTE: set taker withdraw addr to maker's
         #deposit addr this is a quirk of finex api
         self.taker.chainAddresses.setWithdrawAddresses(
            [self.maker.chainAddresses.getDepositAddr()])

      if self.maker.chainAddresses.hasAddresses() and \
         self.taker.chainAddresses.hasAddresses():
         def setDefaultWithdrawAddr(p1, p2):
            depositAddr = p1.chainAddresses.getDepositAddr()
            wtdrAddrs = p2.chainAddresses.getWithdrawAddresses()
            if depositAddr in wtdrAddrs:
               p2.chainAddresses.setDefaultWithdrawAddr(depositAddr)

         setDefaultWithdrawAddr(self.maker, self.taker)
         setDefaultWithdrawAddr(self.taker, self.maker)
      else:
         return

      if self.maker.chainAddresses.hasDefaultWtdrAddr() and \
         self.taker.chainAddresses.hasDefaultWtdrAddr():
         self.loadedAddresses = self.LOAD_ADDRESS_DONE
      else:
         self.loadedAddresses = self.LOAD_ADDRESS_MISMATCH
      await self.processRebalance()

   ##
   async def setup(self):
      #load providers' addresses
      await self.maker.loadAddresses(self.completeSetup)
      await self.taker.loadAddresses(self.completeSetup)

      #get pending withdrawals
      async def wtdrCallback():
         if self.maker.withdrawalsLoaded() and \
            self.taker.withdrawalsLoaded():
            self.loadedWithdrawals = True
            await self.processRebalance()

      await self.maker.loadWithdrawals(wtdrCallback)
      await self.taker.loadWithdrawals(wtdrCallback)

   ## rebalance math ##
   async def assessRebalanceTarget(self):
      #sanity check
      if not self.canAssess():
         return

      ## 1. find total free exposure ##

      #1.a: get cash metrics. Providers that are not ready will
      #     return None
      makerCash = self.maker.getCashMetrics()
      takerCash = self.taker.getCashMetrics()
      if makerCash is None or takerCash is None:
         return

      #1.b: get total cash per provider
      makerTotal = makerCash['total'] + makerCash['pending']
      takerTotal = takerCash['total'] + takerCash['pending']

      #1.c: check values have changed vs existing target
      if self.target != None and not self.target.inTransit():
         if makerTotal == self.target.maker.cash['total'] + self.target.maker.cash['pending'] and \
            takerTotal == self.target.taker.cash['total'] + self.target.taker.cash['pending']:
            #total cash has not changed, nothing to do
            return

      ## 2. find point of equilibrium between providers ##

      #2.a: get total cash across providers
      totalCash = makerTotal + takerTotal

      #2.b: distribute along collateral ratios
      makerRatio = makerCash['ratio'] / \
         (makerCash['ratio'] + takerCash['ratio'])
      makerTarget = totalCash * makerRatio
      takerTarget = totalCash - makerTarget

      ## 3. assess the need for withdrawals ##

      #3.a: apply results
      self.target = RebalanceTarget(self.config,
         self.maker, makerTarget,
         self.taker, takerTarget
      )

      #3.b: progress rebalance target
      await self.processRebalance()

   ## rebalance process ##
   async def processRebalance(self):
      #get a target if we dont have one
      if self.target == None or not self.target.inTransit():
         await self.assessRebalanceTarget()
         return

      #progress the rebalance target state
      step = self.target.progress()
      await self.onEventFunc(None, Rebalance)

      #ignore if we cant withdraw
      if not self.canWithdraw():
         self.target.state = RebalanceTarget.STATE_NO_REBALANCE
         return

      if step == None:
         if self.target.state == RebalanceTarget.STATE_COMPLETED:
            self.target = None
         return

      #process rebalance step
      if step['maker'] != None:
         if CANCEL_PENDING in step['maker']:
            #cancel maker's pending withdrawals
            await self.maker.cancelWithdrawals()

         if WITHDRAW in step['maker']:
            await self.maker.withdraw(step['maker'][WITHDRAW], None)

      if step['taker'] != None:
         if CANCEL_PENDING in step['taker']:
            #cancel maker's pending withdrawals
            await self.taker.cancelWithdrawals()

         if WITHDRAW in step['taker']:
            await self.taker.withdraw(step['taker'][WITHDRAW], None)

################################################################################
class RebalanceStatusReport(RebalanceReport):
   def __init__(self, hedger):
      super().__init__(hedger)
      self.rebalMan = hedger.rebalMan

   def getReadyString(self):
      if self.rebalMan.canWithdraw():
         return "True"

      result = "False"
      if not self.rebalMan.enabled:
         result += " (disabled in config)"
      elif self.rebalMan.loadedAddresses == RebalanceManager.LOAD_ADDRESS_PENDING:
         result += " (waiting on addresses)"
      elif self.rebalMan.loadedAddresses == RebalanceManager.LOAD_ADDRESS_MISMATCH:
         result += " (address mismtach)"
      elif not self.rebalMan.loadedWithdrawals:
         result += " (waiting on withdrawal history)"
      return result

   def getProgressString(self):
      if self.rebalMan.target == None or not self.rebalMan.canWithdraw():
         return "N/A"

      if self.rebalMan.target.state == RebalanceTarget.STATE_CANCELLING_WTDR:
         return "Cancelling past withdrawals"
      elif self.rebalMan.target.state == RebalanceTarget.STATE_WITHDRAWING:
         provider = self.rebalMan.target.maker
         if provider.toWithdraw['amount'] == 0:
            provider = self.rebalMan.target.taker
         result = "Withdrawing from {}: {} usdt".format(
            provider.provider.name, provider.toWithdraw['amount'])
         return result

      return "Idle"

   def __str__(self):
      #status
      result = " |- STATUS:\n"\
         " |  * ready: {}, needs rebalance: {} *\n".format(
            self.getReadyString(),
            self.rebalMan.needsRebalance())

      #addresses
      def setAddresses(provider):
         if provider.chainAddresses.hasDepositAddr():
            depAddr = provider.chainAddresses.getDepositAddr()

         resultStr = " |  * {} *\n".format(provider.name)
         resultStr += " |    withdraw:"

         if not provider.chainAddresses.hasWithdrawAddr():
            resultStr += " N/A\n"
         else:
            addrs = provider.chainAddresses.getWithdrawAddresses()
            for i in range(0, len(addrs)):
               resultStr += " {}\n".format(addrs[i])
               if i < len(addrs) - 1:
                  resultStr += " |             "

         resultStr += " |    deposits: {}\n".format(depAddr)
         return resultStr

      result += " |\n"
      result += " |- ADDRESSES:\n"
      result += setAddresses(self.rebalMan.maker)
      result += setAddresses(self.rebalMan.taker)

      #withdrawals
      result += " |\n"
      result += " |- WITHDRAWALS:\n"
      def setWithdrawals(provider):
         wtdList = provider.getPendingWithdrawals()
         resultStr = " |  * {} *\n".format(provider.name)
         if wtdList == None or len(wtdList) == 0:
            resultStr += " |      N/A\n"
         else:
            for wtd in wtdList:
               resultStr += " |      {}\n".format(wtd)
         return resultStr
      result += setWithdrawals(self.rebalMan.maker)
      result += setWithdrawals(self.rebalMan.taker)

      #target
      result += " |\n"
      result += " |- TARGET: - progress: {} -\n".format(self.getProgressString())
      if self.rebalMan.target == None:
         result += " |    N/A"
      else:
         def setBalances(target):
            cashMetrics = target.provider.getCashMetrics()
            return " |  * {}: balance: {}, pending: {}, target: {}\n".format(
               target.provider.name,
               round(cashMetrics['total'], 2),
               round(cashMetrics['pending'], 2),
               round(target.target, 2))

         result += setBalances(self.rebalMan.target.maker)
         result += setBalances(self.rebalMan.target.taker)
      return result

################################################################################
## Hedger
################################################################################
class SimpleHedger(HedgerFactory):
   required_settings = {
      'hedger' : ['price_ratio', 'max_offer_volume'],
      'rebalance' : ['enable', 'threshold_pct', 'min_amount']
   }

   def __init__(self, config):
      super().__init__("Hedger")

      #check for required config entries
      for k in self.required_settings:
         if k not in config:
            raise HedgerException(f'Missing \"{k}\" in config')

         for kk in self.required_settings[k]:
            if kk not in config[k]:
               raise HedgerException(f'Missing \"{kk}\" in config group \"{k}\"')

      self.config = config
      self.price_ratio = config['hedger']['price_ratio']
      self.max_offer_volume = config['hedger']['max_offer_volume']

      self.offer_refresh_delay = 200 #in milliseconds
      if 'offer_refresh_delay_ms' in config['hedger']:
         self.offer_refresh_delay = config['hedger']['offer_refresh_delay_ms']

      self.offers = []
      self.rebalMan = None

   #############################################################################
   ## price offers methods
   #############################################################################
   async def clearOffers(self, maker):
      if len(self.offers) == 0:
         return

      self.offers = []
      await maker.submitOffers(self.offers)

   ####
   def compareOffers(self, offers):
      if len(offers) != len(self.offers):
         return False

      for i in range(0, len(offers)):
         if self.offers[i].compare(offers[i], self.offer_refresh_delay) == False:
            return False

      return True

   ####
   async def submitOffers(self, maker, taker):
      if not self.isReady():
         await self.clearOffers(maker)
         return

      #figure out long and short buying power for the taker and the maker
      maker_volume = maker.getOpenVolume()
      taker_volume = taker.getOpenVolume()
      if maker_volume is None or taker_volume is None:
         await self.clearOffers(maker)
         return

      '''
      The price at which the maker buys is the price at which
      it sells to the taker, and vice versa.
      Maker ask should be matched with taker bid and so on when
      calculating price streams volume.
      '''
      ask_volume = min(maker_volume['ask'], taker_volume['bid'])
      bid_volume = min(maker_volume['bid'], taker_volume['ask'])

      #cap by max offer volume where applicable
      ask_volume = min(ask_volume, self.max_offer_volume)
      bid_volume = min(bid_volume, self.max_offer_volume)

      #get a price from taker for that volume
      ask = taker.order_book.get_aggregated_ask_price(ask_volume)
      bid = taker.order_book.get_aggregated_bid_price(bid_volume)

      #adjust volume to order book depth
      if ask == None:
         ask_volume = 0
         ask_price = 0
      else:
         ask_volume = min(ask_volume, ask.volume)
         ask_price = round(ask.price * (1 + self.price_ratio), 2)

      if bid == None:
         bid_volume = 0
         bid_price = 0
      else:
         bid_volume = min(bid_volume, bid.volume)
         bid_price = round(bid.price * (1 - self.price_ratio), 2)

      #form the price offers
      offers = []
      try:
         if ask_volume == bid_volume:
               offer = PriceOffer(volume=ask_volume, ask=ask_price, bid=bid_price)
               offers = [offer]
         else:
            if ask_volume != 0:
               offers.append(PriceOffer(volume=ask_volume, ask=ask_price))
            if bid_volume != 0:
               offers.append(PriceOffer(volume=bid_volume, bid=bid_price))
      except OfferException as e:
         logging.debug("failed to instantiate valid offer:\n"
            f"  ask vol: {ask_volume}, price: {ask_price}\n"
            f"  bid vol: {bid_volume}, price: {bid_price}")

      #do not push offers if they didn't change
      if self.compareOffers(offers) == True:
         return

      #submit offers to maker
      self.offers = offers
      await maker.submitOffers(self.offers)

   #############################################################################
   ## exposure & rebalance methods
   #############################################################################
   async def checkExposureSync(self, maker, taker):
      #compare maker and taker exposure
      makerExposure = maker.getExposure()
      takerExposure = taker.getExposure()

      '''
      - A provider that isn't "ready" means it is not able to provide
        functionality for an underlying healthy service.
      - A provider that is "broken" means the underlying service is unable
        to provide the functionality (unhealthy).

      * There is no action to take while a provider is not ready, besides
        trying to ready it. Once it recovers, exposure synchronization
        can be reassessed (we need to be able to query balance and
        exposure for synchronization).

      * A broken provider means the underlying service is dead, we assume
        the exposure is not effective anymore and we need to wipe
        counterparty exposure with the counterparty provider.

      . Changing exposure at the taker is straight forward, so we expect
        we can always zero out the taker when the maker breaks.
      . Changing the maker's exposure is complex. We don't address broken
        taker in this hedger implementation.
      '''

      if takerExposure == None:
         #taker isn't ready, skip exposure sync
         return

      if makerExposure == None:
         if not maker.isBroken():
            #maker is not ready, skip exposure sync
            return

         #maker is broken, wipe taker exposure
         makerExposure = 0

      #NOTE: taker exposure is expected to be the opposite of the maker exposure
      #this is why we add the 2, and expect the diff to be 0
      exposureDiff = makerExposure + takerExposure

      #ignore differences that are less than 100 satoshis
      if abs(exposureDiff) > 0.000001:
         #we need to adjust the taker position by the opposite of the difference
         exposureUpdate = exposureDiff * -1.0

         #TODO: check we have the cash in the taker to enter this position,
         #if not, we need to trigger a rebalance event and reduce our offers
         #on the opposite side of exposureUpdate to get the maker exposure in sync
         #with the taker's capacity

         #update taker position
         await taker.updateExposure(exposureUpdate)

      #report ready state to hedger factory. On first exposure sync, this will
      #set the hedger ready flag. Further calls will have no effect
      self.setReady()
      if self.rebalMan == None:
         self.rebalMan = RebalanceManager(self.config,
            maker, taker, self.onEventFunc)
         await self.rebalMan.setup()

   ####
   def canRebalance(self):
      if not self.isReady():
         return False
      elif self.rebalMan == None:
         return False

      return self.rebalMan.canWithdraw()

   ####
   def needsRebalance(self):
      if not self.isReady():
         return False
      elif self.rebalMan == None:
         return False

      return self.rebalMan.needsRebalance()


   #############################################################################
   ## taker events
   #############################################################################
   async def onTakerOrderBookEvent(self, maker, taker):
      await self.submitOffers(maker, taker)

   ####
   async def onTakerPositionEvent(self, maker, taker):
      # check balance across maker and taker, rebalance if needed
      await self.checkBalanceDistribution(maker, taker)

      # check exposure across maker and taker, resync accordingly
      await self.checkExposureSync(maker, taker)

   #############################################################################
   ## maker events
   #############################################################################
   async def onMakerPositionEvent(self, maker, taker):
      # check balance across maker and taker, rebalance if needed
      await self.checkBalanceDistribution(maker, taker)

      # check exposure across maker and taker, resync accordingly
      await self.checkExposureSync(maker, taker)

      # update offers
      await self.submitOffers(maker, taker)

   #############################################################################
   ## balance events
   #############################################################################
   async def onBalanceEvent(self, maker, taker):
      # check balance across maker and taker, rebalance if needed
      await self.checkBalanceDistribution(maker, taker)

      # update offers
      await self.submitOffers(maker, taker)

   ####
   async def checkBalanceDistribution(self, maker, taker):
      if self.rebalMan != None:
         await self.rebalMan.processRebalance()

   #############################################################################
   ## ready events
   #############################################################################
   async def onReadyEvent(self, maker, taker):
      # update offers
      await self.checkExposureSync(maker, taker)
      await self.submitOffers(maker, taker)

   #############################################################################
   ## rebalance status
   #############################################################################
   def getRebalanceStatus(self):
      if self.rebalMan == None:
         return None
      return RebalanceStatusReport(self)