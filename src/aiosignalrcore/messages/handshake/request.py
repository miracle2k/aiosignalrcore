class HandshakeRequestMessage:
    def __init__(self, protocol: str, version: int) -> None:
        self.protocol = protocol
        self.version = version
