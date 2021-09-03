import json
import logging
from json import JSONEncoder
from typing import Dict, List, Union
from aiosignalrcore.messages.base_message import BaseMessage

from aiosignalrcore.messages.message_type import MessageType
from aiosignalrcore.protocol.abstract import Protocol

_logger = logging.getLogger(__name__)


class MyEncoder(JSONEncoder):
    # https://github.com/PyCQA/pylint/issues/414
    def default(self, o):
        if type(o) is MessageType:
            return o.value
        data = o.__dict__
        if "invocation_id" in data:
            data["invocationId"] = data["invocation_id"]
            del data["invocation_id"]
        if "stream_ids" in data:
            data["streamIds"] = data["stream_ids"]
            del data["stream_ids"]
        return data


class JsonProtocol(Protocol):
    def __init__(self) -> None:
        super().__init__("json", 1, "Text", chr(0x1E))
        self.encoder = MyEncoder()

    def parse_raw_message(self, raw_message: Union[str, bytes]) -> List[BaseMessage]:
        if isinstance(raw_message, bytes):
            raw_message = raw_message.decode()

        raw_messages = [
            record.replace(self.record_separator, "")
            for record in raw_message.split(self.record_separator)
            if record is not None and record != "" and record != self.record_separator
        ]
        result = []
        for item in raw_messages:
            dict_message = json.loads(item)
            if dict_message:
                result.append(self.parse_message(dict_message))
        return result

    def write_message(self, message):
        raise NotImplementedError

    def encode(self, message):
        _logger.debug(self.encoder.encode(message) + self.record_separator)
        return self.encoder.encode(message) + self.record_separator
