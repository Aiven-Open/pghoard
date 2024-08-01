import contextlib
import logging

import sentry_sdk

from pghoard.monitoring.sentry import SentryClient


@contextlib.contextmanager
def patch_sentry_init(events):
    original_init = sentry_sdk.init

    def patched_init(*args, **kwargs):
        kwargs["transport"] = events.append
        kwargs.pop("dsn", None)
        return original_init(*args, **kwargs)

    sentry_sdk.init = patched_init
    try:
        yield
    finally:
        sentry_sdk.init = original_init


def test_missing_config():
    client = SentryClient(config=None)
    client.unexpected_exception(ValueError("hello !"), where="tests")
    client.gauge("something", 123.456)
    client.increase("something")


def test_exception_send():
    events = []
    with patch_sentry_init(events):
        client = SentryClient(config={"dsn": "http://localhost:9000", "tags": {"foo": "bar"}})
    client.unexpected_exception(ValueError("hello !"), where="tests")
    assert len(events) == 1


def test_logging_integration():
    events = []
    with patch_sentry_init(events):
        SentryClient(config={"dsn": "http://localhost:9000", "tags": {"foo": "bar"}})

    logging.warning("Info")
    assert len(events) == 0
    logging.error("Error")
    assert len(events) == 0
    logging.critical("Critical")
    assert len(events) == 1
