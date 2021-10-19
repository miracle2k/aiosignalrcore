import asyncio
import logging
from collections import Callable

# from functools import partial
from typing import Awaitable, Dict, Optional, Union

import aiohttp
import websockets
from websockets.protocol import State

from aiosignalrcore.exceptions import AuthorizationError, HubError
from aiosignalrcore.helpers import Helpers
from aiosignalrcore.messages import CompletionMessage, Message

# from aiosignalrcore.messages import PingMessage
from aiosignalrcore.protocol.abstract import Protocol
from aiosignalrcore.transport.abstract import ConnectionState, Transport

# from aiosignalrcore.transport.websocket.reconnection import ConnectionStateChecker, ReconnectionHandler

_logger = logging.getLogger(__name__)


class WebsocketTransport(Transport):
    def __init__(
        self,
        url: str,
        protocol: Protocol,
        callback: Callable[[Message], Awaitable[None]],
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
        self._callback = callback
        self._headers = headers or {}
        self._skip_negotiation = skip_negotiation

        self._state = ConnectionState.disconnected
        self._connected = asyncio.Event()
        self._handshake = asyncio.Event()

        self._ws: Optional[websockets.WebSocketClientProtocol] = None

        self._open_callback: Optional[Callable[[], Awaitable[None]]] = None
        self._close_callback: Optional[Callable[[], Awaitable[None]]] = None
        self._error_message: Optional[Callable[[CompletionMessage], Awaitable[None]]] = None

        # self.enable_trace = enable_trace
        # self.skip_negotiation = skip_negotiation
        # self.token = None  # auth
        # self.verify_ssl = verify_ssl
        # self.connection_checker = ConnectionStateChecker(partial(self.send, PingMessage()), keep_alive_interval)
        # self.reconnection_handler = reconnection_handler

    def on_open(self, callback: Callable[[], Awaitable[None]]):
        self._open_callback = callback

    def on_close(self, callback: Callable[[], Awaitable[None]]):
        self._close_callback = callback

    def on_error(self, callback: Callable[[CompletionMessage], Awaitable[None]]):
        self._error_callback = callback

    def _set_state(self, state: ConnectionState) -> None:
        if state == ConnectionState.connected:
            self._connected.set()
        else:
            self._connected.clear()
            self._handshake.clear()
        self._state = state

    async def _get_connection(self) -> websockets.WebSocketClientProtocol:
        await self._wait()
        if not self._ws:
            raise RuntimeError
        return self._ws

    async def _wait(self) -> None:
        for _ in range(100):
            await asyncio.wait_for(self._connected.wait(), timeout=10)
            if not self._ws:
                raise RuntimeError
            elif not self._ws.state == State.OPEN:
                await asyncio.sleep(0.1)
            else:
                break

    async def run(self) -> None:
        if not self._skip_negotiation:
            await self._negotiate()

        self._set_state(ConnectionState.connecting)
        _logger.debug("start url:" + self._url)

        await asyncio.gather(
            self._run(),
            # self.connection_checker.run(),
        )

    async def _run(self) -> None:
        max_size = 1_000_000_000

        while True:
            try:
                # TODO: tuning
                async with websockets.connect(
                    self._url,
                    max_size=max_size,
                    extra_headers=self._headers,
                ) as self._ws:
                    await self._on_open()
                    if self._open_callback:
                        await self._open_callback()

                    while True:
                        message = await self._ws.recv()
                        await self._on_raw_message(message)

            except websockets.exceptions.ConnectionClosed:
                await self._on_close()
                if self._close_callback:
                    await self._close_callback()

    async def _negotiate(self):
        negotiate_url = Helpers.get_negotiate_url(self._url)
        _logger.debug("Negotiate url:{0}".format(negotiate_url))

        async with aiohttp.ClientSession() as session:
            async with session.post(negotiate_url, headers=self._headers) as response:
                _logger.debug("Response status code{0}".format(response.status))

                if response.status != 200:
                    raise HubError(response.status) if response.status != 401 else AuthorizationError()

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
        if not self._handshake.is_set():
            self._handshake.set()
            msg = self._protocol.handshake_message()
            assert self._ws
            await self._ws.send(self._protocol.encode(msg))

    async def _on_close(self) -> None:
        _logger.debug("-- web socket close --")
        self._set_state(ConnectionState.disconnected)

    async def _on_raw_message(self, raw_message: Union[str, bytes]) -> None:
        _logger.debug("Message received %s", raw_message)
        if not self._connected.is_set():

            messages = self.evaluate_handshake(raw_message)
            self._set_state(ConnectionState.connected)
            await self._on_open()

            for message in messages:
                await self._on_message(message)

        for message in self._protocol.decode(raw_message):
            await self._on_message(message)

    async def _on_message(self, message: Message) -> None:
        await self._callback(message)

    async def send(self, message: Message) -> None:
        _logger.debug("Sending message {0}".format(message))
        conn = await self._get_connection()
        await conn.send(self._protocol.encode(message))
        # self.connection_checker.reset()
        # if self.reconnection_handler is not None:
        #     self.reconnection_handler.reset()
