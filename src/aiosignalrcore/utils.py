import urllib.parse as parse
from contextlib import suppress
from typing import List

http_schemas = ('http', 'https')
websocket_schemas = ('ws', 'wss')
http_to_ws = {k: v for k, v in zip(http_schemas, websocket_schemas)}
ws_to_http = {k: v for k, v in zip(websocket_schemas, http_schemas)}


def replace_scheme(url: str, ws: bool) -> str:
    parsed_url = parse.urlsplit(url)

    with suppress(KeyError):
        if ws:
            parsed_url._replace(scheme=http_to_ws[parsed_url.scheme])
        else:
            parsed_url._replace(scheme=ws_to_http[parsed_url.scheme])

    return parse.urlunsplit(parsed_url)


def get_negotiate_url(url: str) -> str:
    scheme, netloc, path, query, fragment = parse.urlsplit(url)

    path = path.rstrip('/') + '/negotiate'
    with suppress(KeyError):
        scheme = ws_to_http[scheme]

    return parse.urlunsplit((scheme, netloc, path, query, fragment))


def get_connection_url(url: str, id: List[str]) -> str:
    scheme, netloc, path, query, fragment = parse.urlsplit(url)

    parsed_query = parse.parse_qs(query)
    parsed_query['id'] = id
    query = parse.urlencode(parsed_query, doseq=True)
    with suppress(KeyError):
        scheme = http_to_ws[scheme]

    return parse.urlunsplit((scheme, netloc, path, query, fragment))
