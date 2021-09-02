import asyncio
import logging
from functools import partial
from typing import Any, Callable, Dict, List, Optional, Union

import aiohttp
import websockets

from aiosignalrcore.helpers import Helpers
from aiosignalrcore.hub.errors import HubError, UnAuthorizedHubError
from aiosignalrcore.messages.base_message import BaseMessage
from aiosignalrcore.messages.ping_message import PingMessage
from aiosignalrcore.protocol.base_hub_protocol import BaseHubProtocol
from aiosignalrcore.transport.base_transport import BaseTransport
from aiosignalrcore.transport.websockets.connection import ConnectionState
from aiosignalrcore.transport.websockets.reconnection import ConnectionStateChecker, ReconnectionHandler

_logger = logging.getLogger(__name__)


class WebsocketTransport(BaseTransport):
    def __init__(
        self,
        url: str,
        protocol: BaseHubProtocol,
        headers: Optional[Dict[str, str]] = None,
        skip_negotiation: bool = False,
        # keep_alive_interval: int = 15,
        # reconnection_handler: Optional[ReconnectionHandler] = None,
        # verify_ssl: bool = False,
        # skip_negotiation: bool = False,
        # enable_trace: bool = False,
    ):
        super().__init__()
        self._url = url
        self._protocol = protocol
        self._headers = headers or {}
        self._skip_negotiation = skip_negotiation

        self._state = ConnectionState.disconnected
        self._ws: Optional[websockets.WebSocketClientProtocol] = None
        self._handshake_received = asyncio.Event()

        # self.enable_trace = enable_trace
        # self.skip_negotiation = skip_negotiation
        # self.token = None  # auth
        # self.verify_ssl = verify_ssl
        # self.connection_checker = ConnectionStateChecker(partial(self.send, PingMessage()), keep_alive_interval)
        # self.reconnection_handler = reconnection_handler

    async def run(self) -> None:
        if not self._skip_negotiation:
            await self.negotiate()

        self._state = ConnectionState.connecting
        _logger.debug("start url:" + self._url)

        await asyncio.gather(
            self._run(),
            # self.connection_checker.run(),
        )

    async def _run(self) -> None:
        max_size = 1_000_000_000

        while True:
            try:
                async with websockets.connect(
                    self._url,
                    max_size=max_size,
                    extra_headers=self._headers,
                ) as self._ws:
                    await self._on_open()

                    while True:
                        message = await self._ws.recv()
                        await self._on_raw_message(message)

            except websockets.exceptions.ConnectionClosed:
                self._state = ConnectionState.disconnected
                self._handshake_received.clear()
                await self._on_close()

    async def negotiate(self):
        negotiate_url = Helpers.get_negotiate_url(self._url)
        _logger.debug("Negotiate url:{0}".format(negotiate_url))

        async with aiohttp.ClientSession() as session:
            async with session.post(negotiate_url, headers=self._headers) as response:
                _logger.debug("Response status code{0}".format(response.status))

                if response.status != 200:
                    raise HubError(response.status) if response.status != 401 else UnAuthorizedHubError()

                data = await response.json()

        if "connectionId" in data.keys():
            self._url = Helpers.encode_connection_id(self._url, data["connectionId"])

        # Azure
        if "url" in data.keys() and "accessToken" in data.keys():
            Helpers.get_logger().debug("Azure url, reformat headers, token and url {0}".format(data))
            self._url = data["url"] if data["url"].startswith("ws") else Helpers.http_to_websocket(data["url"])
            self._token = data["accessToken"]
            self._headers = {"Authorization": "Bearer " + self.token}

    def evaluate_handshake(self, message):
        _logger.debug("Evaluating handshake {0}".format(message))
        msg, messages = self._protocol.decode_handshake(message)
        if msg.error:
            raise ValueError("Handshake error {0}".format(msg.error))
        return messages

    async def _on_open(self) -> None:
        _logger.debug("-- web socket open --")
        msg = self._protocol.handshake_message()
        assert self._ws
        await self._ws.send(self._protocol.encode(msg))

    async def _on_close(self) -> None:
        _logger.debug("-- web socket close --")
        self._state = ConnectionState.disconnected
        await self._on_close()

    async def _on_raw_message(self, raw_message: Union[str, bytes]) -> None:
        _logger.debug("Message received %s", raw_message)
        if not self._handshake_received.is_set():
            messages = self.evaluate_handshake(raw_message)
            self._handshake_received.set()
            self._state = ConnectionState.connected
            if callable(self._on_open):
                await self._on_open()

            if len(messages) > 0:
                for message in messages:
                    await self._on_message(message)

        for message in self._protocol.parse_messages(raw_message):
            await self._on_message(message)

    async def _on_message(self, message: BaseMessage) -> None:
        ...

    async def send(self, message) -> None:
        _logger.debug("Sending message {0}".format(message))
        await asyncio.wait_for(self._handshake_received.wait(), timeout=10)
        assert self._ws

        await self._ws.send(self._protocol.encode(message))
        # self.connection_checker.reset()
        # if self.reconnection_handler is not None:
        #     self.reconnection_handler.reset()

    async def _wait(self) ->