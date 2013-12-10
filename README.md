The TweetStream class is just a simple HTTP client for handling the
twitter streaming API.

Usage is pretty simple:

```python
import tweetstream

def callback(message):
    # this will be called every message
    print message

configuration = {
    "twitter_consumer_key": "key",
    "twitter_consumer_secret": "secret",
    "twitter_access_token": "token",
    "twitter_access_token_secret": "secret",
}

stream = tweetstream.TweetStream(configuration)
stream.fetch("/1.1/statuses/filter.json?track=foobar", callback=callback)

# if you aren't on a running ioloop...
from tornado.ioloop import IOLoop
IOLoop.instance().start()
```

The constructor takes two optional arguments, `ioloop` and `clean`.
The `ioloop` argument just lets you specify a specific loop to run on,
and `clean` is just a boolean (False by default) that will strip out
basic data from the twitter message payload.
