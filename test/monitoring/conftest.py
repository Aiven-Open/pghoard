import dataclasses
import http
import http.server
from concurrent.futures import ThreadPoolExecutor
from typing import Iterator

import pytest


@dataclasses.dataclass(frozen=True)
class LoggedRequest:
    path: str
    body: bytes


class LoggingServer(http.server.HTTPServer):
    def __init__(self, *args, **kwargs) -> None:
        super().__init__(*args, **kwargs)
        self.requests: list[LoggedRequest] = []


class LoggingRequestHandler(http.server.BaseHTTPRequestHandler):
    server: LoggingServer

    def do_POST(self) -> None:
        request_body_size = int(self.headers["Content-Length"])
        request_body = self.rfile.read(request_body_size)
        self.server.requests.append(LoggedRequest(path=self.path, body=request_body))
        self.send_response(http.HTTPStatus.FOUND)
        self.end_headers()


@pytest.fixture(scope="module", name="shared_logging_server")
def fixture_shared_logging_server() -> Iterator[LoggingServer]:
    server_address = ("localhost", 50000)
    server = LoggingServer(server_address, RequestHandlerClass=LoggingRequestHandler, bind_and_activate=True)
    with ThreadPoolExecutor(max_workers=1) as executor:
        executor.submit(server.serve_forever)
        try:
            yield server
        finally:
            server.shutdown()


@pytest.fixture(name="logging_server")
def fixture_logging_server(shared_logging_server: LoggingServer) -> Iterator[LoggingServer]:
    try:
        yield shared_logging_server
    finally:
        shared_logging_server.requests.clear()
