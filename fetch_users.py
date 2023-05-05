#!/usr/bin/env python3
"""File to fetch user info so that we get the timezone field from the user.

No longer used - twitter removed timezome info"""

import tweepy
# import datetime
# import random
import json
from tweepy.parsers import JSONParser
# import logging
# logging.basicConfig(level=logging.DEBUG)

config = json.load(open('config.json', 'rb'))
bearer_token = config['bearer_token']
auth = tweepy.OAuth2BearerHandler(bearer_token)
api = tweepy.API(auth, wait_on_rate_limit=True, parser=JSONParser())
