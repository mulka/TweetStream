import os
import asyncio
import logging

import tweetstream_asyncio as tweetstream

configuration = {
    "twitter_consumer_key": os.environ["TWITTER_CONSUMER_KEY"],
    "twitter_consumer_secret": os.environ["TWITTER_CONSUMER_SECRET"],
    "twitter_access_token": os.environ["TWITTER_ACCESS_TOKEN"],
    "twitter_access_token_secret": os.environ["TWITTER_ACCESS_TOKEN_SECRET"],
}

stream = tweetstream.TweetStream(configuration)
stream.fetch("/1.1/statuses/filter.json?track=ferguson")

asyncio.get_event_loop().run_forever()
