import uuid
from typing import Any

from aiosignalrcore.transport.abstract import Transport

from .messages import CompletionClientStreamMessage, InvocationClientStreamMessage, StreamItemMessage


class ClientStream:
    """Client to server streaming
    https://docs.microsoft.com/en-gb/aspnet/core/signalr/streaming?view=aspnetcore-5.0#client-to-server-streaming

        async with ClientStream() as stream:
            for i in range(10)
                stream.send(str(i))
    """

    def __init__(self, transport: Transport, target: str) -> None:
        self.transport: Transport = transport
        self.target: str = target
        self.invocation_id: str = str(uuid.uuid4())

    async def send(self, item: Any) -> None:
        """Send next item to the server"""
        self.transport.send(StreamItemMessage(self.invocation_id, item))

    async def invoke(self) -> None:
        """Start streaming"""
        self.transport.send(InvocationClientStreamMessage([self.invocation_id], self.target, []))

    async def complete(self) -> None:
        """Finish streaming"""
        self.transport.send(CompletionClientStreamMessage(self.invocation_id))
