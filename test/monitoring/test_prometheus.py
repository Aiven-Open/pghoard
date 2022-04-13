import freezegun

from pghoard.monitoring.prometheus import PrometheusClient


@freezegun.freeze_time("2022-01-02 03:04:05")
def test_gauge() -> None:
    client = PrometheusClient(config={})
    client.gauge("something", 123.456)
    assert client.get_metrics() == ["something{} 123.456 1641092645000"]


@freezegun.freeze_time("2022-01-02 03:04:05")
def test_increase() -> None:
    client = PrometheusClient(config={})
    client.increase("something")
    assert client.get_metrics() == ["something{} 1 1641092645000"]


@freezegun.freeze_time("2022-01-02 03:04:05")
def test_custom_increase_value() -> None:
    client = PrometheusClient(config={})
    client.increase("something", inc_value=10)
    assert client.get_metrics() == ["something{} 10 1641092645000"]


@freezegun.freeze_time("2022-01-02 03:04:05")
def test_unexpected_exception() -> None:
    client = PrometheusClient(config={})
    client.unexpected_exception(ValueError("hello !"), where="tests")
    assert client.get_metrics() == ["""pghoard_exception{exception="ValueError",where="tests"} 1 1641092645000"""]


@freezegun.freeze_time("2022-01-02 03:04:05.678")
def test_microseconds_are_ignored_and_truncated() -> None:
    client = PrometheusClient(config={})
    client.gauge("something", 123.456)
    assert client.get_metrics() == ["something{} 123.456 1641092645000"]


def test_identical_metric_overrides_previous_value() -> None:
    client = PrometheusClient(config={})
    with freezegun.freeze_time("2022-01-02 03:04:05"):
        client.gauge("something", 123.456)
    with freezegun.freeze_time("2022-01-02 03:04:35"):
        client.gauge("something", 789.123)
    assert client.get_metrics() == ["something{} 789.123 1641092675000"]


@freezegun.freeze_time("2022-01-02 03:04:05")
def test_metric_with_different_names_are_separated() -> None:
    client = PrometheusClient(config={})
    client.gauge("something", 123.456)
    client.gauge("else", 789.123)
    assert client.get_metrics() == ["something{} 123.456 1641092645000", "else{} 789.123 1641092645000"]


@freezegun.freeze_time("2022-01-02 03:04:05")
def test_metric_with_different_tags_are_separated() -> None:
    client = PrometheusClient(config={})
    client.gauge("something", 123.456, tags={"mark": "one"})
    client.gauge("something", 789.123, tags={"mark": "two"})
    assert client.get_metrics() == [
        """something{mark="one"} 123.456 1641092645000""",
        """something{mark="two"} 789.123 1641092645000""",
    ]


@freezegun.freeze_time("2022-01-02 03:04:05")
def test_metric_names_replaces_dots_and_dashes() -> None:
    client = PrometheusClient(config={})
    client.gauge("a-metric.value", 123)
    assert client.get_metrics() == ["a_metric_value{} 123 1641092645000"]


@freezegun.freeze_time("2022-01-02 03:04:05")
def test_metric_can_have_tags() -> None:
    client = PrometheusClient(config={})
    client.gauge("something", 123, tags={"foo": "bar", "baz": "tog"})
    # tags are sorted
    assert client.get_metrics() == ["""something{baz="tog",foo="bar"} 123 1641092645000"""]


@freezegun.freeze_time("2022-01-02 03:04:05")
def test_metric_can_have_default_tags() -> None:
    client = PrometheusClient(config={"tags": {"foo": "bar"}})
    client.gauge("something", 123)
    assert client.get_metrics() == ["""something{foo="bar"} 123 1641092645000"""]


@freezegun.freeze_time("2022-01-02 03:04:05")
def test_metric_custom_tags_override_defaults() -> None:
    client = PrometheusClient(config={"tags": {"foo": "bar", "baz": "tog"}})
    client.gauge("something", 123, tags={"foo": "notbar"})
    assert client.get_metrics() == ["""something{baz="tog",foo="notbar"} 123 1641092645000"""]
