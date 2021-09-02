import logging
from typing import Callable, Dict, Optional

from .base_hub_connection import BaseHubConnection

_logger = logging.getLogger(__name__)


class AuthHubConnection(BaseHubConnection):
    def __init__(
        self,
        auth_function: Callable,
        headers: Optional[Dict[str, str]] = None,
        **kwargs,
    ):
        self.headers = headers or {}
        self.auth_function = auth_function
        super().__init__(**kwargs)

    async def run(self) -> None:
        # FIXME: Useless wrap
        try:
            _logger.debug("Starting connection ...")
            self.token = self.auth_function()
            _logger.debug("auth function result {0}".format(self.token))
            self.headers["Authorization"] = "Bearer " + self.token
            return await super(AuthHubConnection, self).run()
        except Exception as ex:
            _logger.warning(self.__class__.__name__)
            _logger.warning(str(ex))
            raise ex
