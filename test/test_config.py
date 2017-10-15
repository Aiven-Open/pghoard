# pylint: disable=attribute-defined-outside-init
from .base import PGHoardTestCase
from pghoard.config import validate_config_file
from pghoard.rohmu.errors import InvalidConfigurationError
import pytest


class TestConfig(PGHoardTestCase):

    def test_validate_config_file(self):
        raw_config = self.config_template()
        does_validate = validate_config_file(raw_config)
        assert does_validate
        raw_config["backup_location"] = ""
        with pytest.raises(InvalidConfigurationError):
            validate_config_file(raw_config)
