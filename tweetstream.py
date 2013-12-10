"""
The TweetStream class is just a simple HTTP client for handling the
twitter streaming API.

Usage is pretty simple:

    import tweetstream

    def callback(message):
        # this will be called every message
        print message

    configuration = {
        "twitter_consumer_secret": "ABCDEF1234567890",
        "twitter_consumer_key": "0987654321ABCDEF",
        "twitter_access_token_secret": "1234567890ABCDEF",
        "twitter_access_token": "FEDCBA09123456789"
    }

    stream = tweetstream.TweetStream(configuration)
    stream.fetch("/1/statuses/filter.json?track=foobar", callback=callback)

    # if you aren't on a running ioloop...
    from tornado.ioloop import IOLoop
    IOLoop.instance().start()


The constructor takes two optional arguments, `ioloop` and `clean`.
The `ioloop` argument just lets you specify a specific loop to run on,
and `clean` is just a boolean (False by default) that will strip out
basic data from the twitter message payload.
"""

from tornado.iostream import IOStream, SSLIOStream
from tornado.ioloop import IOLoop
import json
import socket
import time
import oauth2
import urlparse
import logging
from datetime import datetime, timedelta

class MissingConfiguration(Exception):
    """Raised when a configuration value is not found."""
    pass

class TweetStream(object):
    """ Twitter stream connection """

    def __init__(self, configuration, ioloop=None, clean=False):
        """ Just set up the cache list and get first set """
        # prepopulating cache
        self._ioloop = ioloop or IOLoop.instance()
        self._callback = None
        self._error_callback = None
        self._clean_message = clean
        self._configuration = configuration
        consumer_key = self._get_configuration_key("twitter_consumer_key")
        consumer_secret = self._get_configuration_key(
            "twitter_consumer_secret")
        self._consumer = oauth2.Consumer(
            key=consumer_key, secret=consumer_secret)
        access_token = self._get_configuration_key("twitter_access_token")
        access_secret = self._get_configuration_key(
            "twitter_access_token_secret")
        self._token = oauth2.Token(key=access_token, secret=access_secret)
        self._twitter_stream_host = self._get_configuration_key(
            "twitter_stream_host", "stream.twitter.com")
        self._twitter_stream_scheme = self._get_configuration_key(
            "twitter_stream_scheme", "https")
        self._twitter_stream_port = self._get_configuration_key(
            "twitter_stream_port", 443)
        self._twitter_stream = None
        self._stream_restart_scheduled = False
        self._stream_restart_in_process = False
        self._stream_restart_time = None
        self._retry_delay = 5
        self._retry_rate_limited_delay = 60
        self._retry_before_established_delay = .25
        self._timeout_handle = None
        self._stall_timeout_handle = None

    def _get_configuration_key(self, key, default=None):
        """
        Retrieve a configuration option, raising an exception if no
        default is provided.

        """
        configured_value = self._configuration.get(key, default)
        if configured_value is None:
            raise MissingConfiguration("Missing configuration item: %s" % key)
        return configured_value

    def set_error_callback(self, error_callback):
        """Pretty self explanatory."""
        self._error_callback = error_callback

    def fetch(self, path, method="GET", callback=None):
        """ Opens the request """
        parts = urlparse.urlparse(path)
        self._method = method
        self._callback = callback
        self._path = parts.path
        self._full_path = self._path
        self._parameters = {}
        if parts.query:
            self._full_path += "?%s" % parts.query

        # throwing away empty or extra query arguments
        self._parameters = dict([
            (key, value[0]) for key, value in
            urlparse.parse_qs(parts.query).iteritems()
            if value
        ])
        self.schedule_restart()

    def on_error(self, error):
        """ Just a wrapper for the error callback """
        if self._error_callback:
            return self._error_callback(error)
        else:
            raise error

    def open_twitter_stream(self):
        logging.info("open_twitter_stream")
        
        self._stream_restart_scheduled = False

        if self._stream_restart_time is not None and datetime.now() - self._stream_restart_time < timedelta(seconds=5):
            self.schedule_restart(5)
            return

        self._stream_restart_time = datetime.now()
        self._stream_restart_in_process = True

        if self._twitter_stream:
            self.close()

        """ Creates the client and watches stream """
        address_info = socket.getaddrinfo(self._twitter_stream_host,
            self._twitter_stream_port, socket.AF_INET, socket.SOCK_STREAM,
            0, 0)
        af, socktype, proto = address_info[0][:3]
        socket_address = address_info[0][-1]
        sock = socket.socket(af, socktype, proto)
        stream_class = IOStream
        if self._twitter_stream_scheme == "https":
            stream_class = SSLIOStream
        self._twitter_stream = stream_class(sock, io_loop=self._ioloop)
        self._twitter_stream.set_close_callback(self.close_before_established_callback)
        self._twitter_stream.connect(socket_address, self.on_connect)

    def on_connect(self):
        parameters = {
            "oauth_token": self._token.key,
            "oauth_consumer_key": self._consumer.key,
            "oauth_version": "1.0",
            "oauth_nonce": oauth2.generate_nonce(),
            "oauth_timestamp": int(time.time())
        }
        parameters.update(self._parameters)
        request = oauth2.Request(
            method="GET",
            url="%s://%s%s" % (
                self._twitter_stream_scheme,
                self._twitter_stream_host,
                self._path),
            parameters=parameters)
        signature_method = oauth2.SignatureMethod_HMAC_SHA1()
        request.sign_request(signature_method, self._consumer, self._token)
        headers = request.to_header()
        headers["Host"] = self._twitter_stream_host
        headers["User-Agent"] = "TweetStream"
        headers["Accept"] = "*/*"
        # headers["Accept-Encoding"] = "deflate, gzip"
        request = ["GET %s HTTP/1.1" % self._full_path]
        for key, value in headers.iteritems():
            request.append("%s: %s" % (key, value))
        request = "\r\n".join(request) + "\r\n\r\n"
        self._twitter_stream.write(str(request))
        self._twitter_stream.read_until("\r\n\r\n", self.on_headers)

    def on_headers(self, response):
        """ Starts monitoring for results. """
        self._twitter_stream.set_close_callback(lambda: None)
        status_line = response.splitlines()[0]
        response_code = status_line.replace("HTTP/1.1", "")
        response_code = int(response_code.split()[0].strip())
        if response_code != 200:
            if response_code == 420:
                logging.error('stream connect being rate limited. next try in %s seconds' % self._retry_rate_limited_delay)
                self._stream_restart_in_process = False
                self.schedule_restart(self._retry_rate_limited_delay)
                self._retry_rate_limited_delay *= 2
                return
            elif response_code not in [401, 403, 404, 406, 413]:
                logging.error('stream connect failed with response_code: %s. next try in %s seconds' % (response_code, self._retry_delay))
                self._stream_restart_in_process = False
                self.schedule_restart(self._retry_delay)
                if self._retry_delay < 320:
                    self._retry_delay *= 2
                return

            exception_string = "Could not connect: %s\n%s" % (
                status_line, response)
            headers = dict([
                (l.split(":")[0].lower(), ":".join(l.split(":")[1:]))
                for l in response.splitlines()[1:]
            ])
            content_length = int(headers.get("content-length") or 0)
            if not content_length:
                return self.on_error(Exception(exception_string))
            def get_error_body(content):
                full_string = "%s\n%s" % (exception_string, content)
                self.on_error(Exception(full_string))
            return self._twitter_stream.read_bytes(
                content_length, get_error_body)
            
        logging.info("stream connection established")
        self._stream_restart_in_process = False
        self._retry_delay = 5
        self._retry_rate_limited_delay = 60
        self._retry_before_established_delay = .25

        self._twitter_stream.set_close_callback(self.close_callback)
        self.set_stall_timeout()

        self.wait_for_message()

    def wait_for_message(self):
        """ Throw a read event on the stack. """
        if self._twitter_stream.closed():
            logging.error("stream closed by remote host")
            return

        self._twitter_stream.read_until("\r\n", self.on_result)

    def on_result(self, response):
        """ Gets length of next message and reads it """
        if (response.strip() == ""):
            return self.wait_for_message()
        length = int(response.strip(), 16)
        self._twitter_stream.read_bytes(length, self.parse_json)

    def parse_json(self, response):
        self.set_stall_timeout()
        """ Checks JSON message """
        if not response.strip():
            # Empty line, happens sometimes for keep alive
            return self.wait_for_message()
        try:
            response = json.loads(response)
        except ValueError:
            print "Invalid response:"
            print response
            return self.wait_for_message()

        self.parse_response(response)

    def parse_response(self, response):
        """ Parse the twitter message """
        if self._clean_message:
            try:
                text = response["text"]
                name = response["user"]["name"]
                username = response["user"]["screen_name"]
                avatar = response["user"]["profile_image_url_https"]
            except KeyError, exc:
                print "Invalid tweet structure, missing %s" % exc
                return self.wait_for_message()

            response = {
                "type": "tweet",
                "text": text,
                "avatar": avatar,
                "name": name,
                "username": username,
                "time": int(time.time())
            }
        if self._callback:
            self._callback(response)
        self.wait_for_message()

    def schedule_restart(self, seconds=0):
        if not self._stream_restart_scheduled and not self._stream_restart_in_process:
            self._stream_restart_scheduled = True
            if seconds == 0:
                self.open_twitter_stream()
            else:
                self.add_timeout(seconds, self.open_twitter_stream)

    def set_stall_timeout(self):
        self.remove_stall_timeout()
        self._stall_timeout_handle = self._ioloop.add_timeout(timedelta(seconds=90), self.stall_callback)

    def remove_stall_timeout(self):
        if self._stall_timeout_handle:
            self._ioloop.remove_timeout(self._stall_timeout_handle)
            self._stall_timeout_handle = None

    def add_timeout(self, seconds, callback):
        self.remove_timeout()
        self._timeout_handle = self._ioloop.add_timeout(timedelta(seconds=seconds), callback)

    def remove_timeout(self):
        if self._timeout_handle:
            self._ioloop.remove_timeout(self._timeout_handle)
            self._timeout_handle = None

    def close_helper(self):
        self.remove_timeout()
        self.remove_stall_timeout()
        self._twitter_stream.close()

    def close(self):
        self._twitter_stream.set_close_callback(lambda: None)
        self.close_helper()

    def close_before_established_callback(self):
        logging.error('stream closed before establishing a connection')
        self.close()
        self._stream_restart_in_process = False
        self.schedule_restart(self._retry_before_established_delay)
        if self._retry_before_established_delay < 16:
            self._retry_before_established_delay += .25

    def close_callback(self):
        logging.error('close callback')
        self.close_helper()
        self.schedule_restart()

    def stall_callback(self):
        logging.error('stream stalled. restarting')
        self.close()
        self.schedule_restart()

