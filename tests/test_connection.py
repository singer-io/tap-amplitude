import unittest
from unittest.mock import patch
from snowflake.connector.errors import ProgrammingError
from tap_amplitude.connection import connect_with_backoff


class TestSnowflakeConnectionErrors(unittest.TestCase):
    """Unit tests for Snowflake connection retry and error handling."""

    @patch("tap_amplitude.connection.snowflake.connector.connect", side_effect=ProgrammingError("SQL execution internal error"))
    def test_snowflake_programming_error_retry(self, mock_connect):
        """Simulate Snowflake ProgrammingError and verify backoff + retries."""

        dummy_config = {
            "username": "test_user",
            "password": "test_pass",
            "account": "test_account",
            "database": "test_db",
            "warehouse": "test_wh"
        }

        # Ensure the ProgrammingError is raised after retries
        with self.assertRaises(ProgrammingError):
            connect_with_backoff(dummy_config)

        # Ensure it retried exactly 5 times (max_tries)
        self.assertEqual(mock_connect.call_count, 5)
