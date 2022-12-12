import asyncio
import logging

from Factories.Hedger.Factory import HedgerFactory
from Factories.Definitions import PriceOffer, OfferException

################################################################################
class HedgerException(Exception):
   pass

################################################################################
class SimpleHedger(HedgerFactory):
   required_settings = {
      'hedging_settings' : ['price_ratio', 'max_offer_volume']
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

      self.price_ratio = config['hedging_settings']['price_ratio']
      self.max_offer_volume = config['hedging_settings']['max_offer_volume']

      self.offer_refresh_delay = 200 #in milliseconds
      if 'offer_refresh_delay_ms' in config['hedging_settings']:
         self.offer_refresh_delay = config['hedging_settings']['offer_refresh_delay_ms']

      self.offers = []

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

      ask_volume = min(maker_volume['ask'], taker_volume['ask'])
      bid_volume = min(maker_volume['bid'], taker_volume['bid'])

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
   ## exposure methods
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

   #############################################################################
   ## taker events
   #############################################################################
   async def onTakerOrderBookEvent(self, maker, taker):
      await self.submitOffers(maker, taker)

   ####
   async def onTakerPositionEvent(self, maker, taker):
      # check balance across maker and taker, rebalance if needed
      await self.checkBalances(maker, taker)

      # check exposure across maker and taker, resync accordingly
      await self.checkExposureSync(maker, taker)

   #############################################################################
   ## maker events
   #############################################################################
   async def onMakerPositionEvent(self, maker, taker):
      # check balance across maker and taker, rebalance if needed
      await self.checkBalances(maker, taker)

      # check exposure across maker and taker, resync accordingly
      await self.checkExposureSync(maker, taker)

      # update offers
      await self.submitOffers(maker, taker)

   #############################################################################
   ## balance events
   #############################################################################
   async def onBalanceEvent(self, maker, taker):
      # check balance across maker and taker, rebalance if needed
      await self.checkBalances(maker, taker)

      # update offers
      await self.submitOffers(maker, taker)

   ####
   async def checkBalances(self, maker, taker):
      pass

   #############################################################################
   ## ready events
   #############################################################################
   async def onReadyEvent(self, maker, taker):
      # update offers
      await self.checkExposureSync(maker, taker)
      await self.submitOffers(maker, taker)
