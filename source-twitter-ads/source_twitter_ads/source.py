from abc import ABC
from typing import Any, List, Mapping, Tuple
from airbyte_cdk.sources import AbstractSource
from airbyte_cdk.sources.streams import Stream

import requests
from requests_oauthlib import OAuth1

from .stream import (
  Account,
  AdvertisementCampaign,
  Campaign,
  FundingInstrument,
  PromotedTweet,
  PromotedTweetBilling,
  PromotedTweetEngagement,
)


class SourceTwitterFetcher(AbstractSource):

  def auth(self, config: Mapping[str, Any]) -> OAuth1:
    creds=config['credentials']
    return OAuth1(
      creds['consumer_key'], creds['consumer_secret'],
      creds['access_key'], creds['access_secret'])

  def check_connection(self, logger, config) -> Tuple[bool, any]:
    return True, None

  def streams(self, config: Mapping[str, Any]) -> List[Stream]:
    auth = self.auth(config)
    args = {
      "account_ids": config["account_ids"],
      "authenticator": self.auth(config),
    }
    account = Account(**args)
    funding_instrument = FundingInstrument(parent=account, **args)
    campaign = Campaign(parent=account, **args)
    line_item = AdvertisementCampaign(parent=account, **args)
    promoted_tweet = PromotedTweet(parent=account, **args)
    promoted_tweet_billing = PromotedTweetBilling(parent=promoted_tweet, **args)
    promoted_tweet_engagement = PromotedTweetEngagement(parent=promoted_tweet, **args)

    return [
      account,
      funding_instrument,
      campaign,
      line_item,
      promoted_tweet,
      promoted_tweet_billing,
      promoted_tweet_engagement,
    ]
