from abc import ABC
from typing import Any, List, Mapping, Tuple
from datetime import datetime
from airbyte_cdk.sources import AbstractSource
from airbyte_cdk.sources.streams import Stream

import requests
from requests_oauthlib import OAuth1

from .stream import Account, AdvertisementCampaign, PromotedTweetActive, PromotedTweetBilling, PromotedTweetEngagement

DATE_FORMAT = "%Y-%m-%dT%H:%M:%SZ"

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
      "start_time": datetime.strptime(config['start_time'], DATE_FORMAT)
    }
    account = Account(**args)
    campaing = AdvertisementCampaign(parent=account, **args)

    #promoted_tweet_active = PromotedTweetActive(**args)
    #promoted_tweet_billing = PromotedTweetBilling(parent=promoted_tweet_active, **args)
    #promoted_tweet_engagement = PromotedTweetEngagement(parent=promoted_tweet_active, **args)


    return [
      account,
      campaing
        #promoted_tweet_active,
      #promoted_tweet_billing,
      #promoted_tweet_engagement
    ]
