import logging
import uuid
from typing import Any, Callable, Dict, Iterable, List, Optional, Tuple, Union

from aiosignalrcore.hub.handlers import InvocationHandler, StreamHandler
from aiosignalrcore.messages.base_message import BaseMessage
from aiosignalrcore.messages.cancel_invocation_message import CancelInvocationMessage
from aiosignalrcore.messages.close_message import CloseMessage
from aiosignalrcore.messages.completion_message import CompletionMessage
from aiosignalrcore.messages.invocation_message import InvocationMessage
from aiosignalrcore.messages.message_type import MessageType
from aiosignalrcore.messages.stream_invocation_message import StreamInvocationMessage
from aiosignalrcore.messages.stream_item_message import StreamItemMessage
from aiosignalrcore.protocol.abstract import Protocol
from aiosignalrcore.subject import Subject
from aiosignalrcore.transport.websocket import WebsocketTransport

from aiosignalrcore.protocol.json import JsonProtocol

_logger = logging.getLogger(__name__)


class SignalRClient:
    # FIXME: protocol type
    def __init__(
        self,
        url: str,
        protocol: Optional[Protocol] = None,
        headers: Optional[Dict[str, str]] = None,
    ) -> None:
        self._url = url
        self._protocol = protocol or JsonProtocol()
        self._headers = headers or {}
        self._handlers: List[Tuple[str, Callable]] = []
        self._stream_handlers: List[Union[StreamHandler, InvocationHandler]] = []

        self._transport = WebsocketTransport(
            url=self._url,
            protocol=self._protocol,
            headers=self._headers,
        )
        # FIXME
        self._transport._on_message = self._on_message  # type: ignore

    async def run(self) -> None:

        # TODO: If auth...
        # _logger.debug("Starting connection ...")
        # self.token = self.auth_function()
        # _logger.debug("auth function result {0}".format(self.token))
        # self._headers["Authorization"] = "Bearer " + self.token

        _logger.debug("Connection started")
        return await self._transport.run()

    def on(self, event: str, callback_function: Callable) -> None:
        """Register a callback on the specified event
        Args:
            event (string):  Event name
            callback_function (Function): callback function,
                arguments will be binded
        """
        _logger.debug("Handler registered started {0}".format(event))
        self._handlers.append((event, callback_function))

    async def send(self, method: str, arguments: Union[Subject, List[Dict[str, Any]]], on_invocation=None) -> None:
        """Sends a message

        Args:
            method (string): Method name
            arguments (list|Subject): Method parameters
            on_invocation (function, optional): On invocation send callback
                will be raised on send server function ends. Defaults to None.

        Raises:
            ConnectionError: If hub is not ready to send
            TypeError: If arguments are invalid list or Subject
        """
        if isinstance(arguments, list):
            message = InvocationMessage(str(uuid.uuid4()), method, arguments, headers=self._headers)

            if on_invocation:
                self._stream_handlers.append(InvocationHandler(message.invocation_id, on_invocation))

            await self._transport.send(message)

        elif isinstance(arguments, Subject):
            arguments.connection = self
            arguments.target = method
            arguments.start()

        else:
            raise NotImplementedError

    async def _on_message(self, messages: Iterable[BaseMessage]) -> None:
        for message in messages:
            # FIXME: When?
            if message.type == MessageType.invocation_binding_failure:
                raise Exception
                # _logger.error(message)
                # self._on_error(message)

            elif message.type == MessageType.ping:
                pass

            elif isinstance(message, InvocationMessage):
                await self._on_invocation_message(message)

            elif isinstance(message, CloseMessage):
                await self._on_close_message(message)

            elif isinstance(message, CompletionMessage):
                await self._on_completion_message(message)

            elif isinstance(message, StreamItemMessage):
                await self._on_stream_item_message(message)

            elif isinstance(message, StreamInvocationMessage):
                pass

            elif isinstance(message, CancelInvocationMessage):
                await self._on_cancel_invocation_message(message)

            else:
                raise NotImplementedError

    async def stream(self, event, event_params):
        """Starts server streaming
            connection.stream(
            "Counter",
            [len(self.items), 500])\
            .subscribe({
                "next": self.on_next,
                "complete": self.on_complete,
                "error": self.on_error
            })
        Args:
            event (string): Method Name
            event_params (list): Method parameters

        Returns:
            [StreamHandler]: stream handler
        """
        invocation_id = str(uuid.uuid4())
        stream_obj = StreamHandler(event, invocation_id)
        self._stream_handlers.append(stream_obj)
        await self._transport.send(StreamInvocationMessage(invocation_id, event, event_params, headers=self._headers))
        return stream_obj

    async def _on_invocation_message(self, message: InvocationMessage) -> None:
        fired_handlers = list(filter(lambda h: h[0] == message.target, self._handlers))
        if len(fired_handlers) == 0:
            _logger.warning("event '{0}' hasn't fire any handler".format(message.target))
        for _, handler in fired_handlers:
            await handler(message.arguments)

    async def _on_completion_message(self, message: CompletionMessage) -> None:
        if message.error is not None and len(message.error) > 0:
            self._on_error(message)

        # Send callbacks
        fired_stream_handlers = list(
            filter(
                lambda h: h.invocation_id == message.invocation_id,
                self._stream_handlers,
            )
        )

        # Stream callbacks
        for stream_handler in fired_stream_handlers:
            stream_handler.complete_callback(message)

        # unregister handler
        self._stream_handlers = list(
            filter(
                lambda h: h.invocation_id != message.invocation_id,
                self._stream_handlers,
            )
        )

    async def _on_stream_item_message(self, message: StreamItemMessage) -> None:
        fired_handlers = list(
            filter(
                lambda h: h.invocation_id == message.invocation_id,
                self._stream_handlers,
            )
        )
        if len(fired_handlers) == 0:
            _logger.warning("id '{0}' hasn't fire any stream handler".format(message.invocation_id))
        for handler in fired_handlers:
            assert isinstance(handler, StreamHandler)
            handler.next_callback(message.item)

    async def _on_cancel_invocation_message(self, message: CancelInvocationMessage) -> None:
        fired_handlers = list(
            filter(
                lambda h: h.invocation_id == message.invocation_id,
                self._stream_handlers,
            )
        )
        if len(fired_handlers) == 0:
            _logger.warning("id '{0}' hasn't fire any stream handler".format(message.invocation_id))

        for handler in fired_handlers:
            assert isinstance(handler, StreamHandler)
            handler.error_callback(message)

        # unregister handler
        self._stream_handlers = list(
            filter(
                lambda h: h.invocation_id != message.invocation_id,
                self._stream_handlers,
            )
        )

    async def _on_error(self, error: Any) -> None:
        pass

    async def _on_close_message(self, message: CloseMessage) -> None:
        if message.error:
            raise Exception(message.error)
