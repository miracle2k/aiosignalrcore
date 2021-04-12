import asyncio
import websockets
import threading
import requests
import traceback
import uuid
import time
import ssl
from functools import partial
from .reconnection import ConnectionStateChecker
from .connection import ConnectionState
from ...messages.ping_message import PingMessage
from ...hub.errors import HubError, HubConnectionError, UnAuthorizedHubError
from ...protocol.messagepack_protocol import MessagePackHubProtocol
from ...protocol.json_hub_protocol import JsonHubProtocol
from ..base_transport import BaseTransport
from ...helpers import Helpers


class WebSocketsConnection:

    def __init__(self, hub_connection):
        self._hub_connection = hub_connection
        self._ws = None
        self._loop = None

    async def run(self):
        url = self._hub_connection.url
        headers = self._hub_connection.headers
        max_size = 1_000_000_000

        while True:
            try:
                self._ws = await websockets.connect(
                    url,
                    max_size=max_size,
                    extra_headers=headers,
                )
                await self._hub_connection.on_open()
                while True:
                    message = await self._ws.recv()
                    await self._hub_connection.on_message(message)
            except websockets.exceptions.ConnectionClosed as ex:
                self._hub_connection.state = ConnectionState.disconnected
                if self._hub_connection.reconnection_handler is None:
                    if self._on_close is not None and \
                            callable(self._on_close):
                        self._on_close()
                    raise ValueError(str(ex))
                # Connection closed
                await self._hub_connection.handle_reconnect()

    async def send(self, data):
        await self._ws.send(data)

    async def close(self):
        await self._hub_connection.on_close()

        if self._ws is not None:
            await self._ws.close()

        if self._loop is not None:
            self._loop.cancel()


class WebsocketTransport(BaseTransport):
    def __init__(self,
                 url="",
                 headers={},
                 keep_alive_interval=15,
                 reconnection_handler=None,
                 verify_ssl=False,
                 skip_negotiation=False,
                 enable_trace=False,
                 **kwargs):
        super(WebsocketTransport, self).__init__(**kwargs)
        self._ws = None
        self.enable_trace = enable_trace
        self._thread = None
        self.skip_negotiation = skip_negotiation
        self.url = url
        self.headers = headers
        self.handshake_received = False
        self.token = None  # auth
        self.state = ConnectionState.disconnected
        self.connection_alive = False
        self._thread = None
        self._ws = None
        self.verify_ssl = verify_ssl
        self.connection_checker = ConnectionStateChecker(
            partial(self.send, PingMessage()),
            keep_alive_interval
        )
        self.reconnection_handler = reconnection_handler

    def is_running(self):
        return self.state != ConnectionState.disconnected

    async def stop(self):
        if self.state == ConnectionState.connected:
            self.connection_checker.stop()
            await self._ws.close()
            self.state = ConnectionState.disconnected
            self.handshake_received = False

    async def start(self):
        if not self.skip_negotiation:
            self.negotiate()

        if self.state == ConnectionState.connected:
            self.logger.warning("Already connected unable to start")
            return False

        self.state = ConnectionState.connecting
        self.logger.debug("start url:" + self.url)

        self._ws = WebSocketsConnection(self)
        await self._ws.run()

    def negotiate(self):
        negotiate_url = Helpers.get_negotiate_url(self.url)
        self.logger.debug("Negotiate url:{0}".format(negotiate_url))

        response = requests.post(
            negotiate_url, headers=self.headers, verify=self.verify_ssl)
        self.logger.debug(
            "Response status code{0}".format(response.status_code))

        if response.status_code != 200:
            raise HubError(response.status_code) \
                if response.status_code != 401 else UnAuthorizedHubError()

        data = response.json()

        if "connectionId" in data.keys():
            self.url = Helpers.encode_connection_id(
                self.url, data["connectionId"])

        # Azure
        if 'url' in data.keys() and 'accessToken' in data.keys():
            Helpers.get_logger().debug(
                "Azure url, reformat headers, token and url {0}".format(data))
            self.url = data["url"] \
                if data["url"].startswith("ws") else \
                Helpers.http_to_websocket(data["url"])
            self.token = data["accessToken"]
            self.headers = {"Authorization": "Bearer " + self.token}

    def evaluate_handshake(self, message):
        self.logger.debug("Evaluating handshake {0}".format(message))
        msg, messages = self.protocol.decode_handshake(message)
        if msg.error is None or msg.error == "":
            self.handshake_received = True
            self.state = ConnectionState.connected
            if self.reconnection_handler is not None:
                self.reconnection_handler.reconnecting = False
                if not self.connection_checker.running:
                    self.connection_checker.start()
        else:
            self.logger.error(msg.error)
            self.on_socket_error(msg.error)
            self.stop()
            raise ValueError("Handshake error {0}".format(msg.error))
        return messages

    async def on_open(self):
        self.logger.debug("-- web socket open --")
        msg = self.protocol.handshake_message()
        await self.send(msg)

    async def on_close(self):
        self.logger.debug("-- web socket close --")
        self.state = ConnectionState.disconnected
        if self._on_close is not None and callable(self._on_close):
            await self._on_close()

    def on_socket_error(self, error):
        """
        Throws error related on
        https://github.com/websocket-client/websocket-client/issues/449

        Args:
            error ([type]): [description]

        Raises:
            HubError: [description]
        """
        self.logger.debug("-- web socket error --")
        if (type(error) is AttributeError and
                "'NoneType' object has no attribute 'connected'"
                in str(error)):
            url = "https://github.com/websocket-client" + \
                  "/websocket-client/issues/449"
            self.logger.warning(
                "Websocket closing error: issue" +
                url)
            self._on_close()
        else:
            self.logger.error(traceback.format_exc(5, True))
            self.logger.error("{0} {1}".format(self, error))
            self.logger.error("{0} {1}".format(error, type(error)))
            self._on_close()
            raise HubError(error)

    async def on_message(self, raw_message):
        self.logger.debug("Message received{0}".format(raw_message))
        self.connection_checker.last_message = time.time()
        if not self.handshake_received:
            messages = self.evaluate_handshake(raw_message)
            if self._on_open is not None and callable(self._on_open):
                self.state = ConnectionState.connected
                await self._on_open()

            if len(messages) > 0:
                return await self._on_message(messages)

            return []

        return await self._on_message(
            self.protocol.parse_messages(raw_message))

    async def send(self, message, on_invocation=None):
        self.logger.debug("Sending message {0}".format(message))
        try:
            await self._ws.send(
                self.protocol.encode(message)
            )
            self.connection_checker.last_message = time.time()
            if self.reconnection_handler is not None:
                self.reconnection_handler.reset()
        except (
                websockets.exceptions.WebSocketException,
                OSError) as ex:
            self.handshake_received = False
            self.logger.warning("Connection closed {0}".format(ex))
            self.state = ConnectionState.disconnected
            if self.reconnection_handler is None:
                if self._on_close is not None and \
                        callable(self._on_close):
                    self._on_close()
                raise ValueError(str(ex))
            # Connection closed
            self.handle_reconnect()
        except Exception as ex:
            raise ex

    async def handle_reconnect(self):
        self.reconnection_handler.reconnecting = True
        try:
            await self.stop()
            await self.start()
        except Exception as ex:
            self.logger.error(ex)
            sleep_time = self.reconnection_handler.next()
            threading.Thread(
                target=self.deferred_reconnect,
                args=(sleep_time,)
            ).start()

    async def deferred_reconnect(self, sleep_time):
        await asyncio.sleep(sleep_time)
        try:
            if not self.connection_alive:
                self.send(PingMessage())
        except Exception as ex:
            self.logger.error(ex)
            self.reconnection_handler.reconnecting = False
            self.connection_alive = False