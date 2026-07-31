import unittest
from unittest import mock

import attrs

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

    @mock.patch("backoff._sync.time.sleep", return_value=None)
    @mock.patch("tap_amplitude.connection.snowflake.connector.connect")
    def test_connect_with_backoff_retries_on_snowflake_error_then_succeeds(self, mock_connect, _mock_sleep):
        config = self._config()
        transient_error = connection_module.snowflake.connector.Error(msg="temporary failure")
        expected_connection = object()
        mock_connect.side_effect = [transient_error, transient_error, expected_connection]

        result = connect_with_backoff(config)

        self.assertIs(result, expected_connection)
        self.assertEqual(3, mock_connect.call_count)

    @mock.patch("backoff._sync.time.sleep", return_value=None)
    @mock.patch("tap_amplitude.connection.snowflake.connector.connect")
    def test_connect_with_backoff_gives_up_after_max_tries(self, mock_connect, _mock_sleep):
        config = self._config()
        permanent_error = connection_module.snowflake.connector.Error(msg="permanent failure")
        mock_connect.side_effect = permanent_error

        with self.assertRaises(connection_module.snowflake.connector.Error):
            connect_with_backoff(config)

        self.assertEqual(5, mock_connect.call_count)

    @mock.patch("tap_amplitude.connection.snowflake.connector.connect")
    def test_connect_with_backoff_accepts_attrs_asdict_config(self, mock_connect):
        @attrs.define
        class Config:
            username: str
            password: str
            account: str
            database: str
            warehouse: str

        cfg = Config(
            username="user",
            password="pass",
            account="acct",
            database="db",
            warehouse="wh",
        )

        connect_with_backoff(attrs.asdict(cfg))

        mock_connect.assert_called_once_with(
            user="user",
            password="pass",
            account="acct",
            database="db",
            warehouse="wh",
        )
