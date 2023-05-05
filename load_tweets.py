import json
import pandas as pd


tweets = []

with open('tweets.json', 'rt') as f:
    for tweet_str in f:
        tweet = json.loads(tweet_str)
        tweets.append(tweet)
df = pd.json_normalize(tweets)
print(df.columns)
print(df)
