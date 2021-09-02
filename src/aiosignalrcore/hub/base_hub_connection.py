import logging
import uuid
from typing import Callable, Dict, Iterable, List, Optional, Tuple, Union

from aiosignalrcore.hub.handlers import InvocationHandler, StreamHandler
from aiosignalrcore.messages.base_message import BaseMessage
from aiosignalrcore.messages.cancel_invocation_message import CancelInvocationMessage
from aiosignalrcore.messages.completion_message import CompletionMessage
from aiosignalrcore.messages.invocation_message import InvocationMessage
from aiosignalrcore.messages.message_type import MessageType
from aiosignalrcore.messages.stream_invocation_message import StreamInvocationMessage
from aiosignalrcore.messages.stream_item_message import StreamItemMessage
from aiosignalrcore.subject import Subject
from aiosignalrcore.transport.websockets.websocket_transport import WebsocketTransport

_logger = logging.getLogger(__name__)


class BaseHubConnection:
    # FIXME: protocol type
    def __init__(self, url: str, protocol, headers: Optional[Dict[str, str]] = None, **kwargs) -> None:
        self.headers = headers or {}
        self.handlers: List[Tuple[str, Callable]] = []
        self.stream_handlers: List[Union[StreamHandler, InvocationHandler]] = []
        self._on_error = lambda error: _logger.info("on_error not defined {0}".format(error))
        self.transport = WebsocketTransport(url=url, protocol=protocol, headers=headers, on_message=self.on_message, **kwargs)

    async def run(self) -> None:
        _logger.debug("Connection started")
        return await self.transport.run()

    def on_close(self, callback: Callable) -> None:
        """Configures on_close connection callback.
            It will be raised on connection closed event
        connection.on_close(lambda: print("connection closed"))
        Args:
            callback (function): function without params
        """
        self.transport.on_close_callback(callback)

    def on_open(self, callback: Callable) -> None:
        """Configures on_open connection callback.
            It will be raised on connection open event
        connection.on_open(lambda: print(
            "connection opened "))
        Args:
            callback (function): funciton without params
        """
        self.transport.on_open_callback(callback)

    def on_error(self, callback: Callable) -> None:
        """Configures on_error connection callback. It will be raised
            if any hub method throws an exception.
        connection.on_error(lambda data:
            print(f"An exception was thrown closed{data.error}"))
        Args:
            callback (function): function with one parameter.
                A CompletionMessage object.
        """
        self._on_error = callback

    def on(self, event: str, callback_function: Callable) -> None:
        """Register a callback on the specified event
        Args:
            event (string):  Event name
            callback_function (Function): callback function,
                arguments will be binded
        """
        _logger.debug("Handler registered started {0}".format(event))
        self.handlers.append((event, callback_function))

    async def send(self, method: str, arguments: Union[Subject, List[Subject]], on_invocation=None) -> None:
        """Sends a message

        Args:
            method (string): Method name
            arguments (list|Subject): Method parameters
            on_invocation (function, optional): On invocation send callback
                will be raised on send server function ends. Defaults to None.

        Raises:
            HubConnectionError: If hub is not ready to send
            TypeError: If arguments are invalid list or Subject
        """
        if isinstance(arguments, list):
            message = InvocationMessage(str(uuid.uuid4()), method, arguments, headers=self.headers)

            if on_invocation:
                self.stream_handlers.append(InvocationHandler(message.invocation_id, on_invocation))

            await self.transport.send(message)

        elif isinstance(arguments, Subject):
            arguments.connection = self
            arguments.target = method
            arguments.start()

        else:
            raise NotImplementedError

    async def on_message(self, messages: Iterable[BaseMessage]) -> None:
        for message in messages:
            # FIXME: When?
            if message.type == MessageType.invocation_binding_failure:
                _logger.error(message)
                self._on_error(message)

            elif message.type == MessageType.ping:
                pass

            elif isinstance(message, InvocationMessage):
                await self._on_invocation_message(message)

            elif message.type == MessageType.close:
                _logger.info("Close message received from server")
                return

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
        self.stream_handlers.append(stream_obj)
        await self.transport.send(StreamInvocationMessage(invocation_id, event, event_params, headers=self.headers))
        return stream_obj

    async def _on_invocation_message(self, message: InvocationMessage) -> None:
        fired_handlers = list(filter(lambda h: h[0] == message.target, self.handlers))
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
                self.stream_handlers,
            )
        )

        # Stream callbacks
        for stream_handler in fired_stream_handlers:
            stream_handler.complete_callback(message)

        # unregister handler
        self.stream_handlers = list(
            filter(
                lambda h: h.invocation_id != message.invocation_id,
                self.stream_handlers,
            )
        )

    async def _on_stream_item_message(self, message: StreamItemMessage) -> None:
        fired_handlers = list(
            filter(
                lambda h: h.invocation_id == message.invocation_id,
                self.stream_handlers,
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
                self.stream_handlers,
            )
        )
        if len(fired_handlers) == 0:
            _logger.warning("id '{0}' hasn't fire any stream handler".format(message.invocation_id))

        for handler in fired_handlers:
            assert isinstance(handler, StreamHandler)
            handler.error_callback(message)

        # unregister handler
        self.stream_handlers = list(
            filter(
                lambda h: h.invocation_id != message.invocation_id,
                self.stream_handlers,
            )
        )
