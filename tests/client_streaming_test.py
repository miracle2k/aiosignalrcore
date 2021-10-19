from aiosignalrcore.client_stream import ClientStream
from tests.base_test_case import BaseTestCase, Urls


class TestClientStreamMethod(BaseTestCase):
    def test_stream(self):
        self.complete = False
        self.items = list(range(0, 10))
        client_stream = ClientStream()
        self.connection.send("ClientStream", client_stream)
        while len(self.items) > 0:
            client_stream.next(str(self.items.pop()))
        client_stream.complete()
        self.assertTrue(len(self.items) == 0)


class TestClientStreamMethodMsgPack(TestClientStreamMethod):
    def get_connection(self):
        return super().get_connection(msgpack=True)


class TestClientNosslStreamMethodMsgPack(TestClientStreamMethodMsgPack):
    server_url = Urls.server_url_no_ssl


class TestClientNosslStreamMethod(TestClientStreamMethod):
    server_url = Urls.server_url_no_ssl
