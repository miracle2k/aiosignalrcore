from abc import ABC, abstractmethod
from enum import IntEnum, auto
from typing import Union

from aiosignalrcore.messages.base_message import BaseMessage
from aiosignalrcore.protocol.abstract import Protocol


class ConnectionState(IntEnum):
    connecting = auto()
    connected = auto()
    reconnecting = auto()
    disconnected = auto()


class Transport(ABC):
    protocol: Protocol
    state: ConnectionState

    @abstractmethod
    async def run(self) -> None:
        ...

    @abstractmethod
    async def send(self, message: bytes):
        ...

    @abstractmethod
    async def _on_open(self) -> None:
        ...

    @abstractmethod
    async def _on_close(self) -> None:
        ...

    @abstractmethod
    async def _on_raw_message(self, raw_message: Union[str, bytes]) -> None:
        ...

    @abstractmethod
    async def _on_message(self, message: BaseMessage) -> None:
        ...

    @abstractmethod
    async def _wait(self) -> None:
        ...
