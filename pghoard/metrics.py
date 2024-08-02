"""
Interface for monitoring clients

"""
import logging
from dataclasses import dataclass
from typing import Dict, Optional, Type

from pghoard.monitoring import (PrometheusClient, PushgatewayClient, SentryClient, StatsClient)
from pghoard.monitoring.base import MetricsClient

LOG = logging.getLogger(__name__)


@dataclass()
class AvailableClient:
    client_class: Type[MetricsClient]
    config_key: str


class Metrics:
    available_clients = [
        AvailableClient(StatsClient, "statsd"),
        AvailableClient(PrometheusClient, "prometheus"),
        AvailableClient(PushgatewayClient, "pushgateway"),
        AvailableClient(SentryClient, "sentry"),
    ]

    def __init__(self, **configs):
        self.clients = {}

        for client_info in self.available_clients:
            client_config = configs.get(client_info.config_key)
            if isinstance(client_config, dict):
                LOG.info("Initializing monitoring client %s", client_info.config_key)
                self.clients[client_info.config_key] = client_info.client_class(client_config)

    def gauge(self, metric: str, value: float, tags: Optional[Dict[str, str]] = None) -> None:
        for client in self.clients.values():
            client.gauge(metric, value, tags)

    def increase(self, metric: str, inc_value: int = 1, tags: Optional[Dict[str, str]] = None) -> None:
        for client in self.clients.values():
            client.increase(metric, inc_value, tags)

    def unexpected_exception(self, ex: Exception, where: str, tags: Optional[Dict[str, str]] = None) -> None:
        for client in self.clients.values():
            client.unexpected_exception(ex, where, tags)
