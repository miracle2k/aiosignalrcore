import asyncio
import logging
from collections import Callable

# from functools import partial
from typing import Awaitable
from typing import Dict
from typing import Optional
from typing import Union

import aiohttp
import websockets
from websockets.protocol import State

from aiosignalrcore.exceptions import AuthorizationError
from aiosignalrcore.exceptions import HubError
from aiosignalrcore.helpers import Helpers
from aiosignalrcore.messages import CompletionMessage
from aiosignalrcore.messages import Message
from aiosignalrcore.messages import PingMessage

# from aiosignalrcore.messages import PingMessage
from aiosignalrcore.protocol.abstract import Protocol
from aiosignalrcore.transport.abstract import ConnectionState
from aiosignalrcore.transport.abstract import Transport

# from aiosignalrcore.transport.websocket.reconnection import ConnectionStateChecker, ReconnectionHandler

_logger = logging.getLogger('aiosignalrcore.transport')


class WebsocketTransport(Transport):
    def __init__(
        self,
        url: str,
        protocol: Protocol,
        callback: Callable[[Message], Awaitable[None]],
        headers: Optional[Dict[str, str]] = None,
        skip_negotiation: bool = False,
    ):
        super().__init__()
        self._url = url
        self._protocol = protocol
        self._callback = callback
        self._headers = headers or {}
        self._skip_negotiation = skip_negotiation

        self._state = ConnectionState.disconnected
        self._connected = asyncio.Event()
        self._ws: Optional[websockets.WebSocketClientProtocol] = None
        self._open_callback: Optional[Callable[[], Awaitable[None]]] = None
        self._close_callback: Optional[Callable[[], Awaitable[None]]] = None

    def on_open(self, callback: Callable[[], Awaitable[None]]) -> None:
        self._open_callback = callback

    def on_close(self, callback: Callable[[], Awaitable[None]]) -> None:
        self._close_callback = callback

    def on_error(self, callback: Callable[[CompletionMessage], Awaitable[None]]) -> None:
        self._error_callback = callback

    async def _set_state(self, state: ConnectionState) -> None:
        _logger.info('Transport state has changed: %s -> %s', self._state.name, state.name)

        if state == ConnectionState.connecting:
            if self._state != ConnectionState.disconnected:
                raise Exception

            self._connected.clear()

        elif state == ConnectionState.connected:
            if self._state == ConnectionState.connected:
                raise Exception

            self._connected.set()

            if self._open_callback:
                await self._open_callback()

        elif state == ConnectionState.reconnecting:
            if self._state == ConnectionState.disconnected:
                raise Exception

            self._connected.clear()

            if self._close_callback:
                await self._close_callback()

        elif state == ConnectionState.disconnected:
            self._connected.clear()

            if self._close_callback:
                await self._close_callback()

        else:
            raise NotImplementedError

        self._state = state

    async def _get_connection(self) -> websockets.WebSocketClientProtocol:
        await asyncio.wait_for(self._connected.wait(), timeout=10)
        if not self._ws or self._ws.state != State.OPEN:
            raise RuntimeError
        return self._ws

    async def run(self) -> None:
        _logger.info('Running WebSocket transport')
        await self._set_state(ConnectionState.connecting)

        if not self._skip_negotiation:
            await self._negotiate()

        while True:
            try:
                _logger.info('Establishing WebSocket connection')
                # TODO: Tune connection parameters
                protocol = websockets.connect(
                    self._url,
                    max_size=1_000_000_000,
                    extra_headers=self._headers,
                )
                async with protocol as conn:
                    await self._handshake(conn)
                    self._ws = conn
                    await self._set_state(ConnectionState.connected)

                    await asyncio.gather(
                        self._process(conn),
                        self._keepalive(conn),
                    )

            except websockets.exceptions.ConnectionClosed:
                self._ws = None
                await self._set_state(ConnectionState.reconnecting)

    async def _process(self, conn: websockets.WebSocketClientProtocol) -> None:
        while True:
            raw_message = await conn.recv()
            await self._on_raw_message(raw_message)

    async def _keepalive(self, conn: websockets.WebSocketClientProtocol) -> None:
        while True:
            await asyncio.sleep(10)
            await conn.send(self._protocol.encode(PingMessage()))

    async def _handshake(self, conn: websockets.WebSocketClientProtocol) -> None:
        _logger.info('Sending handshake to server')
        our_handshake = self._protocol.handshake_message()
        await conn.send(self._protocol.encode(our_handshake))

        _logger.info('Awaiting handshake from server')
        raw_message = await conn.recv()
        handshake, messages = self._protocol.decode_handshake(raw_message)
        if handshake.error:
            raise ValueError(f'Handshake error: {handshake.error}')
        for message in messages:
            await self._on_message(message)

    async def _negotiate(self) -> None:
        negotiate_url = Helpers.get_negotiate_url(self._url)
        _logger.info('Performang negotiation, URL: %s', negotiate_url)

        async with aiohttp.ClientSession() as session:
            async with session.post(negotiate_url, headers=self._headers) as response:
                if response.status != 200:
                    raise HubError(response.status) if response.status != 401 else AuthorizationError()

                data = await response.json()

        if "connectionId" in data.keys():
            self._url = Helpers.encode_connection_id(self._url, data["connectionId"])

        # Azure
        if "url" in data.keys() and "accessToken" in data.keys():
            _logger.info("Azure url, reformat headers, token and url {0}".format(data))
            self._url = data["url"] if data["url"].startswith("ws") else Helpers.http_to_websocket(data["url"])
            self._token = data["accessToken"]
            self._headers = {"Authorization": "Bearer " + self._token}

    async def _on_raw_message(self, raw_message: Union[str, bytes]) -> None:
        for message in self._protocol.decode(raw_message):
            await self._on_message(message)

    async def _on_message(self, message: Message) -> None:
        await self._callback(message)

    async def send(self, message: Message) -> None:
        conn = await self._get_connection()
        await conn.send(self._protocol.encode(message))
