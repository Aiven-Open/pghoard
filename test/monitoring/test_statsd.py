from test.monitoring.conftest import UdpServer
from typing import Callable, Iterator

import pytest

from pghoard.monitoring.statsd import StatsClient


@pytest.fixture(name="udp_server")
def fixture_udp_server(get_available_port: Callable[[], int]) -> Iterator[UdpServer]:
    with UdpServer(port=get_available_port()) as udp_server:
        yield udp_server


@pytest.mark.parametrize(
    "stats_format,expected", [
        ("telegraf", "something:123.456|g"),
        ("datadog", "something:123.456|g"),
    ]
)
def test_gauge(udp_server: UdpServer, stats_format: str, expected: str) -> None:
    client = StatsClient({"port": udp_server.port, "format": stats_format})
    client.gauge("something", 123.456)
    assert udp_server.get_message() == expected


@pytest.mark.parametrize("stats_format,expected", [
    ("telegraf", "something:1|c"),
    ("datadog", "something:1|c"),
])
def test_increase(udp_server: UdpServer, stats_format: str, expected: str) -> None:
    client = StatsClient({"port": udp_server.port, "format": stats_format})
    client.increase("something")
    assert udp_server.get_message() == expected


@pytest.mark.parametrize("stats_format,expected", [
    ("telegraf", "something:10|c"),
    ("datadog", "something:10|c"),
])
def test_custom_increase_value(udp_server: UdpServer, stats_format: str, expected: str) -> None:
    client = StatsClient({"port": udp_server.port, "format": stats_format})
    client.increase("something", inc_value=10)
    assert udp_server.get_message() == expected


@pytest.mark.parametrize(
    "stats_format,expected", [
        ("telegraf", "pghoard.exception,exception=ValueError,where=tests:1|c"),
        ("datadog", "pghoard.exception:1|c|#exception:ValueError,where:tests"),
    ]
)
def test_unexpected_exception(udp_server: UdpServer, stats_format: str, expected: str) -> None:
    client = StatsClient({"port": udp_server.port, "format": stats_format})
    client.unexpected_exception(ValueError("hello !"), where="tests")
    assert udp_server.get_message() == expected


@pytest.mark.parametrize(
    "stats_format,expected", [
        ("telegraf", "something,baz=tog,foo=bar:123|g"),
        ("datadog", "something:123|g|#baz:tog,foo:bar"),
    ]
)
def test_metric_can_have_tags(udp_server: UdpServer, stats_format: str, expected: str) -> None:
    client = StatsClient({"port": udp_server.port, "format": stats_format})
    client.gauge("something", 123, tags={"foo": "bar", "baz": "tog"})
    # tags are sorted
    assert udp_server.get_message() == expected


def test_datadog_tag_values_can_be_none(udp_server: UdpServer) -> None:
    client = StatsClient({"port": udp_server.port, "format": "datadog"})
    client.gauge("something", 123, tags={"foo": None, "bar": None})
    assert udp_server.get_message() == "something:123|g|#bar,foo"


@pytest.mark.parametrize(
    "stats_format,expected", [
        ("telegraf", "something,baz=tog,foo=bar:123|g"),
        ("datadog", "something:123|g|#baz:tog,foo:bar"),
    ]
)
def test_metric_can_have_default_tags(udp_server: UdpServer, stats_format: str, expected: str) -> None:
    client = StatsClient({"port": udp_server.port, "format": stats_format, "tags": {"foo": "bar", "baz": "tog"}})
    client.gauge("something", 123)
    assert udp_server.get_message() == expected


@pytest.mark.parametrize(
    "stats_format,expected", [
        ("telegraf", "something,baz=tog,foo=notbar:123|g"),
        ("datadog", "something:123|g|#baz:tog,foo:notbar"),
    ]
)
def test_metric_custom_tags_override_defaults(udp_server: UdpServer, stats_format: str, expected: str) -> None:
    client = StatsClient({"port": udp_server.port, "format": stats_format, "tags": {"foo": "bar", "baz": "tog"}})
    client.gauge("something", 123, tags={"foo": "notbar"})
    assert udp_server.get_message() == expected


def test_none_host_disables_monitoring(udp_server: UdpServer) -> None:
    client = StatsClient({"port": udp_server.port, "host": None})
    client.gauge("something", 123, tags={"foo": "notbar"})
    assert not udp_server.has_message()


def test_none_port_disables_monitoring(udp_server: UdpServer) -> None:
    client = StatsClient({"port": None, "host": "localhost"})
    client.gauge("something", 123, tags={"foo": "notbar"})
    assert not udp_server.has_message()
