"""
Interface for monitoring clients

"""
import pghoard


class Metrics(object):
    def __init__(self, **configs):
        self.clients = self._init_clients(configs)

    def _init_clients(self, configs):
        clients = []

        if not isinstance(configs, dict):
            return clients

        map_client = pghoard.mapping.clients
        for k, config in configs.items():
            if isinstance(config, dict) and k in map_client:
                path, classname = map_client[k]
                mod = __import__(path, fromlist=[classname])
                klass = getattr(mod, classname)
                clients.append(klass(config))
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
