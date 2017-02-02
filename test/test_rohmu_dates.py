"""
rohmu test case

Copyright (c) 2017 Ohmu Ltd
See LICENSE for details
"""
from pghoard.rohmu.dates import parse_timestamp
import datetime
import dateutil.tz


def test_parse_timestamp():
    local_aware = datetime.datetime.now(dateutil.tz.tzlocal())

    str_local_aware_naive = local_aware.isoformat().split("+", 1)[0]
    str_local_aware_named = "{} {}".format(str_local_aware_naive, local_aware.tzname())

    assert parse_timestamp(str_local_aware_named) == local_aware
    local_naive = parse_timestamp(str_local_aware_named, with_tz=False, assume_local=True)
    assert local_naive == local_aware.replace(tzinfo=None)

    str_unknown_aware = "2017-02-02 12:00:00 XYZ"
    unknown_aware_utc = parse_timestamp(str_unknown_aware)
    assert unknown_aware_utc.tzinfo == datetime.timezone.utc
    assert unknown_aware_utc.isoformat() == "2017-02-02T12:00:00+00:00"

    if local_aware.tzname() in ["EET", "EEST"]:
        unknown_aware_local = parse_timestamp(str_unknown_aware, assume_local=True)
        assert unknown_aware_local.tzinfo == dateutil.tz.tzlocal()
        assert unknown_aware_local.isoformat() == "2017-02-02T12:00:00+02:00"
