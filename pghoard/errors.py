"""
pghoard exception classes

Copyright (c) 2015 Ohmu Ltd
See LICENSE for details
"""


class Error(Exception):
    """generic pghoard exception"""


class InvalidConfigurationError(Error):
    """invalid configuration"""
