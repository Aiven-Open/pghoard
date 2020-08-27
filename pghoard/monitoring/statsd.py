"""
StatsD client

Supports telegraf's statsd protocol extension for 'key=value' tags:

  https://github.com/influxdata/telegraf/tree/master/plugins/inputs/statsd

"""
import socket


class StatsClient:
    def __init__(self, config):
        self._dest_addr = (config.get("host", "127.0.0.1"), config.get("port", 8125))
        self._socket = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        self._tags = config.get("tags", {})
        self._message_format = config.get("format", "telegraf")

    def gauge(self, metric, value, tags=None):
        self._send(metric, b"g", value, tags)

    def increase(self, metric, inc_value=1, tags=None):
        self._send(metric, b"c", inc_value, tags)

    def timing(self, metric, value, tags=None):
        self._send(metric, b"ms", value, tags)

    def unexpected_exception(self, ex, where, tags=None):
        all_tags = {
            "exception": ex.__class__.__name__,
            "where": where,
        }
        all_tags.update(tags or {})
        self.increase("pghoard.exception", tags=all_tags)

    def _send(self, metric, metric_type, value, tags):
        if None in self._dest_addr:
            # stats sending is disabled
            return

        # telegraf format: "user.logins,service=payroll,region=us-west:1|c"
        # datadog format: metric.name:value|type|@sample_rate|#tag1:value,tag2
        #                 http://docs.datadoghq.com/guides/dogstatsd/#datagram-format

        parts = [metric.encode("utf-8"), b":", str(value).encode("utf-8"), b"|", metric_type]
        send_tags = self._tags.copy()
        send_tags.update(tags or {})
        if self._message_format == "datadog":
            for index, (tag, val) in enumerate(send_tags.items()):
                if index == 0:
                    separator = "|#"
                else:
                    separator = ","
                if val is None:
                    pattern = "{}{}"
                else:
                    pattern = "{}{}:{}"
                parts.append(pattern.format(separator, tag, val).encode("utf-8"))
        else:
            for tag, val in send_tags.items():
                parts.insert(1, ",{}={}".format(tag, val).encode("utf-8"))

        self._socket.sendto(b"".join(parts), self._dest_addr)
