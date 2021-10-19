import logging
from typing import Callable
from typing import Dict

_logger = logging.getLogger(__name__)


class StreamHandler:
    def __init__(self, event: str, invocation_id: str) -> None:
        self.event = event
        self.invocation_id = invocation_id
        self.next_callback = lambda _: _logger.warning("next stream handler fired, no callback configured")
        self.complete_callback = lambda _: _logger.warning("next complete handler fired, no callback configured")
        self.error_callback = lambda _: _logger.warning("next error handler fired, no callback configured")

    def subscribe(self, subscribe_callbacks: Dict[str, Callable]) -> None:
        error = " subscribe object must be a dict like {0}".format({"next": None, "complete": None, "error": None})

        if subscribe_callbacks is None or type(subscribe_callbacks) is not dict:
            raise TypeError(error)

        if "next" not in subscribe_callbacks or "complete" not in subscribe_callbacks or "error" not in subscribe_callbacks:
            raise KeyError(error)

        if (
            not callable(subscribe_callbacks["next"])
            or not callable(subscribe_callbacks["complete"])
            or not callable(subscribe_callbacks["error"])
        ):
            raise ValueError("Suscribe callbacks must be functions")

        self.next_callback = subscribe_callbacks["next"]
        self.complete_callback = subscribe_callbacks["complete"]
        self.error_callback = subscribe_callbacks["error"]


class InvocationHandler:
    def __init__(self, invocation_id: str, complete_callback: Callable):
        self.invocation_id = invocation_id
        self.complete_callback = complete_callback
