import asyncio
from functools import partial

import aiohttp
import websockets

from ...helpers import Helpers
from ...hub.errors import HubError, UnAuthorizedHubError
from ...messages.ping_message import PingMessage
from ..base_transport import BaseTransport
from .connection import ConnectionState
from .reconnection import ConnectionStateChecker


class WebsocketTransport(BaseTransport):
    def __init__(
        self,
        url="",
        headers={},
        keep_alive_interval=15,
        reconnection_handler=None,
        verify_ssl=False,
        skip_negotiation=False,
        enable_trace=False,
        **kwargs,
    ):
        super(WebsocketTransport, self).__init__(**kwargs)
        self.enable_trace = enable_trace
        self.skip_negotiation = skip_negotiation
        self.url = url
        self.headers = headers
        self.token = None  # auth
        self.state = ConnectionState.disconnected
        self.handshake_received = asyncio.Event()
        self.verify_ssl = verify_ssl
        self.connection_checker = ConnectionStateChecker(partial(self.send, PingMessage()), keep_alive_interval)
        self.reconnection_handler = reconnection_handler
        self._ws = None

    async def run(self):
        if not self.skip_negotiation:
            await self.negotiate()

        self.state = ConnectionState.connecting
        self.logger.debug("start url:" + self.url)

        await asyncio.gather(
            self._run(),
            self.connection_checker.run(),
        )

    async def _run(self) -> None:
        max_size = 1_000_000_000

        while True:
            try:
                async with websockets.connect(
                    self.url,
                    max_size=max_size,
                    extra_headers=self.headers,
                ) as self._ws:
                    await self.on_open()

                    while True:
                        message = await self._ws.recv()
                        await self.on_message(message)

            except websockets.exceptions.ConnectionClosed:
                self.state = ConnectionState.disconnected
                self.handshake_received.clear()
                await self.on_close()

    async def negotiate(self):
        negotiate_url = Helpers.get_negotiate_url(self.url)
        self.logger.debug("Negotiate url:{0}".format(negotiate_url))

        async with aiohttp.ClientSession() as session:
            response = session.post(negotiate_url, headers=self.headers, verify=self.verify_ssl)
        self.logger.debug("Response status code{0}".format(response.status_code))

        if response.status_code != 200:
            raise HubError(response.status_code) if response.status_code != 401 else UnAuthorizedHubError()

        data = response.json()

        if "connectionId" in data.keys():
            self.url = Helpers.encode_connection_id(self.url, data["connectionId"])

        # Azure
        if "url" in data.keys() and "accessToken" in data.keys():
            Helpers.get_logger().debug("Azure url, reformat headers, token and url {0}".format(data))
            self.url = data["url"] if data["url"].startswith("ws") else Helpers.http_to_websocket(data["url"])
            self.token = data["accessToken"]
            self.headers = {"Authorization": "Bearer " + self.token}

    def evaluate_handshake(self, message):
        self.logger.debug("Evaluating handshake {0}".format(message))
        msg, messages = self.protocol.decode_handshake(message)
        if msg.error:
            raise ValueError("Handshake error {0}".format(msg.error))
        return messages

    async def on_open(self):
        self.logger.debug("-- web socket open --")
        msg = self.protocol.handshake_message()
        await self._ws.send(self.protocol.encode(msg))

    async def on_close(self):
        self.logger.debug("-- web socket close --")
        self.state = ConnectionState.disconnected
        if self._on_close is not None and callable(self._on_close):
            await self._on_close()

    async def on_message(self, raw_message):
        self.logger.debug("Message received{0}".format(raw_message))
        if not self.handshake_received.is_set():
            messages = self.evaluate_handshake(raw_message)
            self.handshake_received.set()
            self.state = ConnectionState.connected
            if callable(self._on_open):
                await self._on_open()

            if len(messages) > 0:
                return await self._on_message(messages)
            return []

        return await self._on_message(self.protocol.parse_messages(raw_message))

    async def send(self, message):
        self.logger.debug("Sending message {0}".format(message))
        await asyncio.wait_for(self.handshake_received.wait(), timeout=10)

        await self._ws.send(self.protocol.encode(message))
        self.connection_checker.reset()
        if self.reconnection_handler is not None:
            self.reconnection_handler.reset()
