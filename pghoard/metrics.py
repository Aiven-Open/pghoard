"""
Interface for monitoring clients

Supports Statsd

"""
from pghoard.statsd import StatsClient # noqa pylint: disable=unused-import
from pghoard import mapping


class Metrics(object):
    def __init__(self, **configs):
        self.clients = self._init_clients(configs)

    def _init_clients(self, configs):
        clients = []

        if not isinstance(configs, dict):
            return clients

        for k, config in configs.items():
            if isinstance(config, dict) and k in mapping.clients:
                client_class = globals()[mapping.clients[k]]
                client = client_class(config)
                clients.append(client)
        return clients

    def gauge(self, metric, value, tags=None):
        for client in self.clients:
            client.gauge(metric, value, tags)

    def increase(self, metric, inc_value=1, tags=None):
        for client in self.clients:
            client.increase(metric, inc_value, tags)

    def timing(self, metric, value, tags=None):
        for client in self.clients:
            client.timing(metric, value, tags)

    def unexpected_exception(self, ex, where, tags=None):
        for client in self.clients:
            client.unexpected_exception(ex, where, tags)
