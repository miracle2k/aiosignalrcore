import sys
import time

sys.path.append("./")
import logging

from aiosignalrcore.client_stream import ClientStream
from aiosignalrcore.hub_connection_builder import SignalRClient


def input_with_default(input_text, default_value):
    value = input(input_text.format(default_value))
    return default_value if value is None or value.strip() == "" else value


server_url = input_with_default("Enter your server url(default: {0}): ", "wss://localhost:5001/chatHub")

client = (
    SignalRClient()
    .with_url(server_url, options={"verify_ssl": False})
    .configure_logging(logging.DEBUG)
    .with_automatic_reconnect(
        {
            "type": "interval",
            "keep_alive_interval": 10,
            "intervals": [1, 3, 5, 6, 7, 87, 3],
        }
    )
    .build()
)
hub_connection.start()
time.sleep(10)


def bye(error, x):
    if error:
        print("error {0}".format(x))
    else:
        print("complete! ")
    global hub_connection
    hub_connection.stop()
    sys.exit(0)


iteration = 0
client_stream = ClientStream()


def interval_handle():
    global iteration
    iteration += 1
    client_stream.next(str(iteration))
    if iteration == 10:
        client_stream.complete()


hub_connection.send("ClientStream", client_stream)

while iteration != 10:
    interval_handle()
    time.sleep(0.5)
