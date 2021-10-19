import json
from abc import ABC, abstractmethod
from typing import Any, Dict, Iterable, Tuple, Union

# TODO: mapping of message types to classes
from aiosignalrcore.messages import CancelInvocationMessage  # 5
from aiosignalrcore.messages import CloseMessage  # 7
from aiosignalrcore.messages import CompletionMessage  # 3
from aiosignalrcore.messages import InvocationMessage  # 1
from aiosignalrcore.messages import PingMessage  # 6
from aiosignalrcore.messages import StreamInvocationMessage  # 4
from aiosignalrcore.messages import StreamItemMessage  # 2
from aiosignalrcore.messages import HandshakeRequestMessage, HandshakeResponseMessage, Message, MessageType


class Protocol(ABC):
    def __init__(self, protocol: str, version: int, transfer_format: str, record_separator: str):
        self.protocol = protocol
        self.version = version
        self.transfer_format = transfer_format
        self.record_separator = record_separator

    @abstractmethod
    def parse_raw_message(self, raw_message: Union[str, bytes]) -> Iterable[Message]:
        ...

    @abstractmethod
    def write_message(self, message: Message):
        ...

    @abstractmethod
    def encode(self, message: Union[Message, HandshakeRequestMessage]) -> bytes:
        ...

    @staticmethod
    def parse_message(dict_message: Dict[str, Any]) -> Message:
        message_type = MessageType(dict_message.pop('type', 'close'))

        if message_type is MessageType.invocation:
            dict_message["invocation_id"] = dict_message.pop("invocationId", None)
            return InvocationMessage(**dict_message)
        elif message_type is MessageType.stream_item:
            return StreamItemMessage(**dict_message)
        elif message_type is MessageType.completion:
            dict_message["invocation_id"] = dict_message.pop("invocationId", None)
            return CompletionMessage(**dict_message, error=dict_message.get("error", None))
        elif message_type is MessageType.stream_invocation:
            return StreamInvocationMessage(**dict_message)
        elif message_type is MessageType.cancel_invocation:
            return CancelInvocationMessage(**dict_message)
        elif message_type is MessageType.ping:
            return PingMessage()
        elif message_type is MessageType.close:
            dict_message["allow_reconnect"] = dict_message.pop("allowReconnect", None)
            return CloseMessage(**dict_message)
        else:
            raise NotImplementedError

    def decode_handshake(self, raw_message: str) -> Tuple[HandshakeResponseMessage, Iterable[Message]]:
        messages = raw_message.split(self.record_separator)
        messages = list(filter(bool, messages))
        data = json.loads(messages[0])
        idx = raw_message.index(self.record_separator)
        return (
            HandshakeResponseMessage(data.get("error", None)),
            self.parse_raw_message(raw_message[idx + 1 :]) if len(messages) > 1 else [],
        )

    def handshake_message(self) -> HandshakeRequestMessage:
        return HandshakeRequestMessage(self.protocol, self.version)
