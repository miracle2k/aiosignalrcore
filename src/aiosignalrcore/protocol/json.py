import json
import logging
from functools import partial
from json import JSONEncoder
from operator import ne
from typing import Any, Dict, List, Union

from aiosignalrcore.messages import HandshakeMessage, Message, MessageType
from aiosignalrcore.protocol.abstract import Protocol

_logger = logging.getLogger(__name__)


class MessageEncoder(JSONEncoder):
    # https://github.com/PyCQA/pylint/issues/414
    def default(self, obj: Union[Message, MessageType]) -> Union[int, Dict[str, Any]]:
        if isinstance(obj, MessageType):
            return obj.value
        return obj.dump()


class JSONProtocol(Protocol):
    def __init__(self) -> None:
        # TODO: What does this constant mean?
        super().__init__("json", 1, "Text", chr(0x1E))
        self.encoder = MessageEncoder()

    def decode(self, raw_message: Union[str, bytes]) -> List[Message]:
        if isinstance(raw_message, bytes):
            raw_message = raw_message.decode()

        raw_messages = raw_message.split(self.record_separator)
        messages: List[Message] = []

        for item in raw_messages:
            if item in ("", self.record_separator):
                continue

            dict_message = json.loads(item)
            if dict_message:
                messages.append(self.parse_message(dict_message))

        return messages

    def encode(self, message: Union[Message, HandshakeMessage]) -> str:
        _logger.debug(self.encoder.encode(message) + self.record_separator)
        return self.encoder.encode(message) + self.record_separator
