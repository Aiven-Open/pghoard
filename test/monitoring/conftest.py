import dataclasses
import http
import http.server
import selectors
import socket
from concurrent.futures import ThreadPoolExecutor
from types import TracebackType
from typing import Callable, Iterator, Type

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
def fixture_shared_logging_server(
    get_available_port: Callable[[], int]
) -> Iterator[LoggingServer]:
    server_address = ("localhost", get_available_port())
    server = LoggingServer(
        server_address,
        RequestHandlerClass=LoggingRequestHandler,
        bind_and_activate=True,
    )
    with ThreadPoolExecutor(max_workers=1) as executor:
        executor.submit(server.serve_forever)
        try:
            yield server
        finally:
            server.shutdown()


@pytest.fixture(name="logging_server")
def fixture_logging_server(
    shared_logging_server: LoggingServer,
) -> Iterator[LoggingServer]:
    try:
        yield shared_logging_server
    finally:
        shared_logging_server.requests.clear()


class UdpServer:
    def __init__(self, port: int) -> None:
        self.port = port
        self.socket = socket.socket(family=socket.AF_INET, type=socket.SOCK_DGRAM)

    def __enter__(self) -> "UdpServer":
        self.socket.bind(("localhost", self.port))
        return self

    def __exit__(
        self, exc_type: Type, exc_val: BaseException, exc_tb: TracebackType
    ) -> None:
        self.socket.close()

    def has_message(self) -> bool:
        selector = selectors.DefaultSelector()
        selector.register(self.socket, selectors.EVENT_READ)
        try:
            return len(selector.select(timeout=-1)) > 0
        finally:
            selector.unregister(self.socket)

    def get_message(self) -> str:
        return self.socket.recv(2048).decode()
