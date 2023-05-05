#!/usr/bin/env python3
"""Read through tweets.json and get place info for any place that doesn't
have it.

Note: This part of the API has a 75 requests/15 min limit. Painfully slow.
Find another way."""

import tweepy
# import datetime
# import random
import time
import json
from tweepy.parsers import JSONParser
# import logging
# logging.basicConfig(level=logging.DEBUG)

config = json.load(open('config.json', 'rb'))
bearer_token = config['bearer_token']
auth = tweepy.OAuth2BearerHandler(bearer_token)
api = tweepy.API(auth, wait_on_rate_limit=True, parser=JSONParser())

places_looked_up = set()

with open('places.json', 'rt') as places_fh:
    for place_str in places_fh:
        id_ = json.loads(place_str)['id']
        places_looked_up.add(id_)

print(f'{len(places_looked_up)} places loaded')

with open('tweets.json', 'rt') as tweets_fh:
    for tweets_str in tweets_fh:
        tweet = json.loads(tweets_str)
        if 'place' not in tweet:
            # Fetched before place inclusion
            continue
        id_ = tweet['place']['id']
        places_looked_up.add(id_)

print(f'{len(places_looked_up)} places loaded')

with open('places.json', 'at') as places_fh:
    with open('tweets.json', 'rt') as tweets_fh:
        for tweet_str in tweets_fh:
            tweet = json.loads(tweet_str)
            place_id = tweet['tweet']['geo']['place_id']
            if place_id in places_looked_up:
                # Already have this place
                continue
            print(f'looking up {place_id}')
            try:
                place = api.geo_id(place_id)
            except tweepy.errors.NotFound as e:
                places_looked_up.add(place_id)
                print(e)
                time.sleep(1)
                continue
            json.dump(place, places_fh)
            places_fh.write('\n')
            places_fh.flush()
            places_looked_up.add(place_id)
            print(f"Lookup {place['name']}")
            time.sleep(1)
