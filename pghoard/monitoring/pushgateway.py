"""
Prometheus Pushgateway client

"""
import socket

import requests


class PushgatewayClient:
    def __init__(self, config):
        self._endpoint = config.get("endpoint", "")
        self._job = config.get("job", "pghoard")
        self._instance = config.get("instance", "")
        self._tags = config.get("tags", {})

    def gauge(self, metric, value, tags=None):
        self._send(metric, "gauge", value, tags)

    def increase(self, metric, inc_value=1, tags=None):
        self._send(metric, "counter", inc_value, tags)

    def unexpected_exception(self, ex, where, tags=None):
        all_tags = {
            "exception": ex.__class__.__name__,
            "where": where,
        }
        all_tags.update(tags or {})
        self.increase("pghoard.exception", tags=all_tags)

    def _send(self, metric, metric_type, value, tags):
        tags = {**self._tags, **tags} if tags else {**self._tags}

        if len(self._endpoint) == 0:
            return

        instance = self._instance if self._instance else tags.get("site", socket.gethostname())

        data = self._build_data(metric, metric_type, value, tags)
        requests.post("{}/metrics/job/{}/instance/{}".format(self._endpoint, self._job, instance), data=data)

    def _build_data(self, metric, metric_type, value, tags):
        metric = metric.replace(".", "_").replace("-", "_")
        tag_list = []
        for k, v in sorted(tags.items()):
            tag_list.append("{}=\"{}\"".format(k, v))

        encoded_tags = "{{{}}}".format(",".join(tag_list))
        return """# TYPE {0} {1}
{0}{2} {3}
""".format(metric, metric_type, encoded_tags, value)
