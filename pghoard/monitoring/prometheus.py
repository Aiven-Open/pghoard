"""
Prometheus client (used to create a Prometheus endpoint)

"""


class PrometheusClient:
    def __init__(self, config):
        self._tags = config.get("tags", {})

    def gauge(self, metric, value, tags=None):
        self._update(metric, value, tags)

    def increase(self, metric, inc_value=1, tags=None):
        self._update(metric, inc_value, tags)

    def timing(self, metric, value, tags=None):
        self._update(metric, value, tags)

    def unexpected_exception(self, ex, where, tags=None):
        pass

    def _update(self, metric, value, tags):
        metric = metric.replace(".", "_")
        tags = {**self._tags, **tags}
        tag_list = []
        for k in sorted(tags.keys()):
            tag_list.append("{}=\"{}\"".format(k, tags[k]))
        encoded_tags = "{{{}}}".format(",".join(tag_list))
        formatted_metric = "{}{}".format(metric, encoded_tags)

        f = open("/tmp/pghoard-prometheus.txt", "a+")
        lines = []
        f.seek(0, 0)
        updated = False
        for line in f.readlines():
            cur_metric = line.split(" ")
            if formatted_metric == cur_metric[0]:
                updated = True
                lines.append("{} {}\r\n".format(formatted_metric, value))
            else:
                lines.append(line)
        if not updated:
            lines.append("{} {}\r\n".format(formatted_metric, value))
        f.truncate(0)
        f.writelines(lines)
        f.close()
