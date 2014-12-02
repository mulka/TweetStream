import json
import time
import asyncio
import logging
from datetime import datetime
from urllib.parse import urlparse, parse_qs, urlencode

import oauthlib.oauth1

@asyncio.coroutine
def read_until(delimeter, reader, buf):
    delimeter = delimeter
    reader = reader

    while True:
        line = yield from reader.readline()
        if not line:
            return (None, buf)
        line = line.decode('utf-8').rstrip()
        if not line:
            continue
        length = int(line, 16)
        # logging.error(length)
        line = yield from reader.read(length)
        # logging.error(line)
        end_line = yield from reader.read(2)
        assert end_line == b'\r\n'
        buf = buf + line
        pos = buf.find(delimeter)
        if pos != -1:
            rv = buf[:pos].decode('utf-8')
            buf = buf[pos + len(delimeter):]
            return (rv, buf)

class TweetStream(object):
    def __init__(self, configuration):
        self._configuration = configuration
        self._consumer_key = self._get_configuration_key("twitter_consumer_key")
        self._consumer_secret = self._get_configuration_key("twitter_consumer_secret")
        access_token = self._get_configuration_key("twitter_access_token")
        access_secret = self._get_configuration_key("twitter_access_token_secret")
        self.set_token(access_token, access_secret)
        self._twitter_stream_host = self._get_configuration_key("twitter_stream_host", "stream.twitter.com")
        self._twitter_stream_scheme = self._get_configuration_key("twitter_stream_scheme", "https")
        self._twitter_stream_port = self._get_configuration_key("twitter_stream_port", 443)

    def set_token(self, access_token, access_secret):
        self._oauth_client = oauthlib.oauth1.Client(
            self._consumer_key, client_secret=self._consumer_secret,
            resource_owner_key=access_token, resource_owner_secret=access_secret)

    def _get_configuration_key(self, key, default=None):
        configured_value = self._configuration.get(key, default)
        if configured_value is None:
            raise MissingConfiguration("Missing configuration item: %s" % key)
        return configured_value

    def fetch(self, path, method="GET"):
        parts = urlparse(path)
        self._method = method
        self._path = parts.path
        self._full_path = self._path
        self._parameters = {}
        if parts.query:
            self._full_path += "?%s" % parts.query

        # throwing away empty or extra query arguments
        self._parameters = dict([
            (key, value[0]) for key, value in
            parse_qs(parts.query).items()
            if value
        ])
        asyncio.async(self.open_twitter_stream())

    @asyncio.coroutine
    def open_twitter_stream(self):
        logging.info("open_twitter_stream")

        reader, writer = yield from asyncio.open_connection(
            self._twitter_stream_host,
            self._twitter_stream_port,
            ssl=(self._twitter_stream_scheme == "https"),
        )

        url = "%s://%s%s?%s" % (
            self._twitter_stream_scheme,
            self._twitter_stream_host,
            self._path,
            urlencode(self._parameters))
        uri, headers, body = self._oauth_client.sign(url)
        headers["Host"] = self._twitter_stream_host
        headers["User-Agent"] = "TweetStream"
        headers["Accept"] = "*/*"
        # headers["Accept-Encoding"] = "deflate, gzip"
        request = ["GET %s HTTP/1.1" % self._full_path]
        for key, value in headers.items():
            request.append("%s: %s" % (key, value))
        request = "\r\n".join(request) + "\r\n\r\n"
        writer.write(request.encode('latin-1'))

        while True:
            line = yield from reader.readline()
            line = line.decode('latin-1').rstrip()
            if not line:
                break

        buf = b''

        while True:
            json_str, buf = yield from read_until(b'\r\n', reader, buf)
            if len(buf):
                logging.error('extra buf: %s' % buf)
            if json_str is None:
                break
            tweet = json.loads(json_str)
            if 'limit' in tweet:
                logging.error('limit: %s' % tweet['limit']['track'])
            else:
                logging.error(tweet['text'])

        writer.close()

        asyncio.get_event_loop().stop()
