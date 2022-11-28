"""
pghoard

Copyright (c) 2015 Ohmu Ltd
See LICENSE for details
"""
import json
from http.client import HTTPConnection

from pytest import raises


class TestIPV6WebServer:
    # verify that existing behaviour of listening on all IPV4 addresses if an empty http_address is
    # specified
    def test_ipv4_endpoint_with_empty_listen_address(self, pghoard_empty_listen_address):
        pghoard_empty_listen_address.write_backup_state_to_json_file()
        conn = HTTPConnection(host="127.0.0.1", port=pghoard_empty_listen_address.config["http_port"])
        conn.request("GET", "/status")
        response = conn.getresponse()
        response_parsed = json.loads(response.read().decode("utf-8"))
        assert response.status == 200
        assert response_parsed["startup_time"] is not None

    # an empty http_address does not make pghoard listen on IPV6
    def test_ipv6_endpoint_fails_with_empty_listen_address(self, pghoard_empty_listen_address):
        pghoard_empty_listen_address.write_backup_state_to_json_file()
        conn = HTTPConnection(host="::1", port=pghoard_empty_listen_address.config["http_port"])
        with raises(ConnectionRefusedError):
            conn.request("GET", "/status")

    def test_ipv4_endpoint_with_ipv4_hostname_listen_address(self, pghoard_ipv4_hostname):
        pghoard_ipv4_hostname.write_backup_state_to_json_file()
        conn = HTTPConnection(host="127.0.0.1", port=pghoard_ipv4_hostname.config["http_port"])
        conn.request("GET", "/status")
        response = conn.getresponse()
        response_parsed = json.loads(response.read().decode("utf-8"))
        assert response.status == 200
        assert response_parsed["startup_time"] is not None

    # an empty http_address does not make pghoard listen on IPV6
    def test_ipv6_endpoint_fails_with_ipv4_hostname_listen_address(self, pghoard_ipv4_hostname):
        pghoard_ipv4_hostname.write_backup_state_to_json_file()
        conn = HTTPConnection(host="::1", port=pghoard_ipv4_hostname.config["http_port"])
        with raises(ConnectionRefusedError):
            conn.request("GET", "/status")

    # a IPV6 wildcard (::) supports connecting with the IPV4 lookback address (and in fact any IPV4 interface)
    def test_ipv4_endpoint_with_wildcard_ipv6_listen_address(self, pghoard_ipv6_wildcard):
        pghoard_ipv6_wildcard.write_backup_state_to_json_file()
        conn = HTTPConnection(host="127.0.0.1", port=pghoard_ipv6_wildcard.config["http_port"])
        conn.request("GET", "/status")
        response = conn.getresponse()
        response_parsed = json.loads(response.read().decode("utf-8"))
        assert response.status == 200
        assert response_parsed["startup_time"] is not None

    def test_ipv6_endpoint_with_wildcard_ipv6_listen_address(self, pghoard_ipv6_wildcard):
        pghoard_ipv6_wildcard.write_backup_state_to_json_file()
        conn = HTTPConnection(host="::1", port=pghoard_ipv6_wildcard.config["http_port"])
        conn.request("GET", "/status")
        response = conn.getresponse()
        response_parsed = json.loads(response.read().decode("utf-8"))
        assert response.status == 200
        assert response_parsed["startup_time"] is not None

    def test_ipv6_endpoint_with_loopback_ipv6_listen_address(self, pghoard_ipv6_loopback):
        pghoard_ipv6_loopback.write_backup_state_to_json_file()
        conn = HTTPConnection(host="::1", port=pghoard_ipv6_loopback.config["http_port"])
        conn.request("GET", "/status")
        response = conn.getresponse()
        response_parsed = json.loads(response.read().decode("utf-8"))
        assert response.status == 200
        assert response_parsed["startup_time"] is not None

    # You cannot connect to pghoard on the IPV4 loopback if it was started with the IPV6 loopback address
    def test_ipv4_endpoint_fails_with_loopback_ipv6_listen_address(self, pghoard_ipv6_loopback):
        pghoard_ipv6_loopback.write_backup_state_to_json_file()
        conn = HTTPConnection(host="127.0.0.1", port=pghoard_ipv6_loopback.config["http_port"])
        with raises(ConnectionRefusedError):
            conn.request("GET", "/status")
