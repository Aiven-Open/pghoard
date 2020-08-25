"""
rohmu - timestamp handling

Copyright (c) 2017 Ohmu Ltd
See LICENSE for details
"""
import datetime

import dateutil.parser
import dateutil.tz


def parse_timestamp(ts, *, with_tz=True, assume_local=False):
    """Parse a given timestamp and return a datetime object with or without tzinfo.

    If `with_tz` is False and we can't parse a timezone from the timestamp the datetime object is returned
    as-is and we assume the timezone is whatever was requested.  If `with_tz` is False and we can parse a
    timezone, the timestamp is converted to either local or UTC time based on `assume_local` after which tzinfo
    is stripped and the timestamp is returned.

    When `with_tz` is True and there's a timezone in the timestamp we return it as-is.  If `with_tz` is True
    but we can't parse a timezone we add either local or UTC timezone to the datetime based on `assume_local`.
    """
    dt = dateutil.parser.parse(ts)

    if with_tz is False:
        if not dt.tzinfo:
            return dt

        tz = dateutil.tz.tzlocal() if assume_local else datetime.timezone.utc
        return dt.astimezone(tz).replace(tzinfo=None)

    if dt.tzinfo:
        return dt

    tz = dateutil.tz.tzlocal() if assume_local else datetime.timezone.utc
    return dt.replace(tzinfo=tz)
