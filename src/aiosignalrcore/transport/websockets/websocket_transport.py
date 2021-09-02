import asyncio
from functools import partial
from typing import Any, Dict, List, Optional
import logging
import aiohttp
import websockets

from aiosignalrcore.helpers import Helpers
from aiosignalrcore.hub.errors import HubError, UnAuthorizedHubError
from aiosignalrcore.messages.ping_message import PingMessage
from aiosignalrcore.transport.base_transport import BaseTransport
from aiosignalrcore.transport.websockets.connection import ConnectionState
from aiosignalrcore.transport.websockets.reconnection import ConnectionStateChecker, ReconnectionHandler

_logger = logging.getLogger(__name__)

class WebsocketTransport(BaseTransport):
    def __init__(
        self,
        url: str,
        headers: Optional[Dict[str, str]] = None,
        keep_alive_interval: int = 15,
        reconnection_handler: Optional[ReconnectionHandler] = None,
        verify_ssl: bool = False,
        skip_negotiation: bool = False,
        enable_trace: bool = False,
        **kwargs,
    ):
        super().__init__(**kwargs)
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
        self._ws: Optional[websockets.WebSocketClientProtocol] = None

    async def run(self) -> None:
        if not self.skip_negotiation:
            await self.negotiate()

        self.state = ConnectionState.connecting
        _logger.debug("start url:" + self.url)

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
        _logger.debug("Negotiate url:{0}".format(negotiate_url))

        async with aiohttp.ClientSession() as session:
            async with session.post(negotiate_url, headers=self.headers) as response:
                _logger.debug("Response status code{0}".format(response.status))

                if response.status != 200:
                    raise HubError(response.status) if response.status != 401 else UnAuthorizedHubError()

                data = await response.json()

        if "connectionId" in data.keys():
            self.url = Helpers.encode_connection_id(self.url, data["connectionId"])

        # Azure
        if "url" in data.keys() and "accessToken" in data.keys():
            Helpers.get_logger().debug("Azure url, reformat headers, token and url {0}".format(data))
            self.url = data["url"] if data["url"].startswith("ws") else Helpers.http_to_websocket(data["url"])
            self.token = data["accessToken"]
            self.headers = {"Authorization": "Bearer " + self.token}

    def evaluate_handshake(self, message):
        _logger.debug("Evaluating handshake {0}".format(message))
        msg, messages = self.protocol.decode_handshake(message)
        if msg.error:
            raise ValueError("Handshake error {0}".format(msg.error))
        return messages

    async def on_open(self):
        _logger.debug("-- web socket open --")
        msg = self.protocol.handshake_message()
        await self._ws.send(self.protocol.encode(msg))

    async def on_close(self):
        _logger.debug("-- web socket close --")
        self.state = ConnectionState.disconnected
        if self._on_close is not None and callable(self._on_close):
            await self._on_close()

    async def on_message(self, raw_message) -> List[Any]:
        _logger.debug("Message received{0}".format(raw_message))
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

    async def send(self, message) -> None:
        _logger.debug("Sending message {0}".format(message))
        await asyncio.wait_for(self.handshake_received.wait(), timeout=10)

        await self._ws.send(self.protocol.encode(message))
        self.connection_checker.reset()
        if self.reconnection_handler is not None:
            self.reconnection_handler.reset()
