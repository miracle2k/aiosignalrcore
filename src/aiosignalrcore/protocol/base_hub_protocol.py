import json
from abc import ABC, abstractmethod
from typing import Any, Iterable, Iterator, Tuple, Union

from aiosignalrcore.messages.base_message import BaseMessage

from ..messages.cancel_invocation_message import CancelInvocationMessage  # 5
from ..messages.close_message import CloseMessage  # 7
from ..messages.completion_message import CompletionMessage  # 3
from ..messages.handshake.request import HandshakeRequestMessage
from ..messages.handshake.response import HandshakeResponseMessage
from ..messages.invocation_message import InvocationMessage  # 1
from ..messages.message_type import MessageType
from ..messages.ping_message import PingMessage  # 6
from ..messages.stream_invocation_message import StreamInvocationMessage  # 4
from ..messages.stream_item_message import StreamItemMessage  # 2


class BaseHubProtocol(ABC):
    def __init__(self, protocol: str, version: int, transfer_format: str, record_separator: str):
        self.protocol = protocol
        self.version = version
        self.transfer_format = transfer_format
        self.record_separator = record_separator

    @abstractmethod
    def parse_messages(self, raw_message: Union[str, bytes]) -> Iterable[BaseMessage]:
        ...

    @abstractmethod
    def write_message(self, hub_message: BaseMessage):
        ...

    @abstractmethod
    def encode(self, message):
        ...

    @staticmethod
    def get_message(dict_message):
        if 'type' in dict_message:
            message_type = MessageType(dict_message['type'])
        else:
            message_type = MessageType.close 

        # FIXME: Copypaste
        dict_message["invocation_id"] = dict_message.get("invocationId", None)
        dict_message["headers"] = dict_message.get("headers", {})
        dict_message["error"] = dict_message.get("error", None)
        dict_message["result"] = dict_message.get("result", None)
        if message_type is MessageType.invocation:
            return InvocationMessage(**dict_message)
        if message_type is MessageType.stream_item:
            return StreamItemMessage(**dict_message)
        if message_type is MessageType.completion:
            return CompletionMessage(**dict_message)
        if message_type is MessageType.stream_invocation:
            return StreamInvocationMessage(**dict_message)
        if message_type is MessageType.cancel_invocation:
            return CancelInvocationMessage(**dict_message)
        if message_type is MessageType.ping:
            return PingMessage()
        if message_type is MessageType.close:
            return CloseMessage(**dict_message)

    def decode_handshake(self, raw_message: str) -> Tuple[HandshakeResponseMessage, Any]:
        messages = raw_message.split(self.record_separator)
        messages = list(filter(lambda x: x != "", messages))
        data = json.loads(messages[0])
        idx = raw_message.index(self.record_separator)
        return (
            HandshakeResponseMessage(data.get("error", None)),
            self.parse_messages(raw_message[idx + 1 :]) if len(messages) > 1 else [],
        )

    def handshake_message(self) -> HandshakeRequestMessage:
        return HandshakeRequestMessage(self.protocol, self.version)
