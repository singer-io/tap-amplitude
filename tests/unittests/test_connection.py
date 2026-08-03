import unittest
from unittest import mock

import tap_amplitude.connection as connection_module
from tap_amplitude.connection import connect_with_backoff


class TestConnection(unittest.TestCase):
    @staticmethod
    def _config():
        return {
            "username": "user",
            "password": "pass",
            "account": "acct",
            "database": "db",
            "warehouse": "wh",
        }

    @mock.patch("tap_amplitude.connection.snowflake.connector.connect")
    def test_connect_with_backoff_uses_expected_config_keys(self, mock_connect):
        config = self._config()

        connect_with_backoff(config)

        mock_connect.assert_called_once_with(
            user="user",
            password="pass",
            account="acct",
            database="db",
            warehouse="wh",
        )
