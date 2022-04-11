from test.monitoring.conftest import LoggedRequest, LoggingServer
from typing import List, Sequence

import pytest

from pghoard.monitoring.pushgateway import PushgatewayClient


def get_lines(logged_requests: Sequence[LoggedRequest]) -> List[str]:
    return [line for logged_request in logged_requests for line in logged_request.body.decode().split("\n") if line != ""]


@pytest.fixture(name="gateway_client")
def fixture_gateway_client(logging_server: LoggingServer) -> PushgatewayClient:
    return PushgatewayClient({
        "endpoint": f"http://{logging_server.server_name}:{logging_server.server_port}",
        "job": "test_job",
        "instance": "test_server",
    })


def test_gauge(gateway_client: PushgatewayClient, logging_server: LoggingServer) -> None:
    gateway_client.gauge("something", 123456)
    assert get_lines(logging_server.requests) == [
        "# TYPE something gauge",
        "something{} 123456",
    ]


def test_increase(gateway_client: PushgatewayClient, logging_server: LoggingServer) -> None:
    gateway_client.increase("something")
    assert get_lines(logging_server.requests) == [
        "# TYPE something counter",
        "something{} 1",
    ]


def test_custom_increase_value(gateway_client: PushgatewayClient, logging_server: LoggingServer) -> None:
    gateway_client.increase("something", 10)
    assert get_lines(logging_server.requests) == [
        "# TYPE something counter",
        "something{} 10",
    ]


def test_unexpected_exception(gateway_client: PushgatewayClient, logging_server: LoggingServer) -> None:
    gateway_client.unexpected_exception(ValueError("hello !"), where="tests")
    assert get_lines(logging_server.requests) == [
        "# TYPE pghoard_exception counter",
        """pghoard_exception{exception="ValueError",where="tests"} 1""",
    ]


def test_identical_metric_follows_previous_value(gateway_client: PushgatewayClient, logging_server: LoggingServer) -> None:
    gateway_client.gauge("something", 123.456)
    gateway_client.gauge("something", 789.123)
    assert get_lines(logging_server.requests) == [
        "# TYPE something gauge",
        "something{} 123.456",
        "# TYPE something gauge",
        "something{} 789.123",
    ]


def test_metric_with_different_names_are_separated(gateway_client: PushgatewayClient, logging_server: LoggingServer) -> None:
    gateway_client.gauge("something", 123.456)
    gateway_client.gauge("else", 789.123)
    assert get_lines(logging_server.requests) == [
        "# TYPE something gauge",
        "something{} 123.456",
        "# TYPE else gauge",
        "else{} 789.123",
    ]


def test_metric_with_different_tags_are_separated(gateway_client: PushgatewayClient, logging_server: LoggingServer) -> None:
    gateway_client.gauge("something", 123.456, tags={"mark": "one"})
    gateway_client.gauge("something", 789.123, tags={"mark": "two"})
    assert get_lines(logging_server.requests) == [
        "# TYPE something gauge",
        """something{mark="one"} 123.456""",
        "# TYPE something gauge",
        """something{mark="two"} 789.123""",
    ]


def test_metric_names_replaces_dots_and_dashes(gateway_client: PushgatewayClient, logging_server: LoggingServer) -> None:
    gateway_client.gauge("a-metric.value", 123)
    assert get_lines(logging_server.requests) == ["# TYPE a_metric_value gauge", "a_metric_value{} 123"]


def test_metric_can_have_tags(gateway_client: PushgatewayClient, logging_server: LoggingServer) -> None:
    gateway_client.gauge("something", 123, tags={"foo": "bar", "baz": "tog"})
    # tags are sorted
    assert get_lines(logging_server.requests) == [
        "# TYPE something gauge",
        """something{baz="tog",foo="bar"} 123""",
    ]


def test_metric_can_have_default_tags(logging_server: LoggingServer) -> None:
    client = PushgatewayClient(
        config={
            "endpoint": f"http://{logging_server.server_name}:{logging_server.server_port}",
            "job": "test_job",
            "instance": "test_server",
            "tags": {
                "foo": "bar"
            }
        }
    )
    client.gauge("something", 123)
    assert get_lines(logging_server.requests) == ["# TYPE something gauge", """something{foo="bar"} 123"""]


def test_metric_custom_tags_override_defaults(logging_server: LoggingServer) -> None:
    client = PushgatewayClient(
        config={
            "endpoint": f"http://{logging_server.server_name}:{logging_server.server_port}",
            "job": "test_job",
            "instance": "test_server",
            "tags": {
                "foo": "bar",
                "baz": "tog"
            }
        }
    )
    client.gauge("something", 123, tags={"foo": "notbar"})
    assert get_lines(logging_server.requests) == [
        "# TYPE something gauge",
        """something{baz="tog",foo="notbar"} 123""",
    ]


def test_empty_endpoints_disables_monitoring(logging_server: LoggingServer) -> None:
    client = PushgatewayClient(config={"endpoint": ""})
    client.gauge("something", 123, tags={"foo": "notbar"})
    assert get_lines(logging_server.requests) == []
