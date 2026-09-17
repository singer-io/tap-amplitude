import os

from tap_tester.base_suite_tests.base_case import BaseCase

from utils import ensure_amplitude_test_tables


class AmplitudeBaseTest(BaseCase):
    start_date = "2019-01-01T00:00:00Z"

    EVENTS_STREAM = "PUBLIC-TAP_TEST_EVENTS"
    MERGE_STREAM = "PUBLIC-TAP_TEST_MERGE_EVENTS"

    @staticmethod
    def tap_name():
        return "tap-amplitude"

    @staticmethod
    def get_type():
        return "platform.amplitude"

    @classmethod
    def expected_metadata(cls):
        return {
            cls.EVENTS_STREAM: {
                cls.PRIMARY_KEYS: {"UUID"},
                cls.REPLICATION_METHOD: cls.INCREMENTAL,
                cls.REPLICATION_KEYS: {"SERVER_UPLOAD_TIME"},
                cls.OBEYS_START_DATE: False,
                cls.API_LIMIT: 100,
            },
            cls.MERGE_STREAM: {
                cls.PRIMARY_KEYS: {"MERGE_ID"},
                cls.REPLICATION_METHOD: cls.INCREMENTAL,
                cls.REPLICATION_KEYS: {"MERGE_EVENT_TIME"},
                cls.OBEYS_START_DATE: False,
                cls.API_LIMIT: 100,
            },
        }

    @staticmethod
    def get_credentials():
        return {
            "account": os.getenv("TAP_SNOWFLAKE_ACCOUNT"),
            "warehouse": os.getenv("TAP_SNOWFLAKE_WAREHOUSE"),
            "database": os.getenv("TAP_SNOWFLAKE_DATABASE"),
            "username": os.getenv("TAP_SNOWFLAKE_USERNAME"),
            "password": os.getenv("TAP_SNOWFLAKE_PASSWORD"),
        }

    def get_properties(self, original: bool = True):
        return {"start_date": self.start_date}

    def expected_stream_names(self):
        return set(self.expected_metadata().keys())

    def expected_automatic_fields(self):
        return {
            stream: properties.get(self.PRIMARY_KEYS, set())
            | properties.get(self.REPLICATION_KEYS, set())
            for stream, properties in self.expected_metadata().items()
        }

    @classmethod
    def setUpClass(cls):
        super().setUpClass()

        missing_envs = [
            env
            for env in [
                "TAP_SNOWFLAKE_USERNAME",
                "TAP_SNOWFLAKE_PASSWORD",
                "TAP_SNOWFLAKE_ACCOUNT",
                "TAP_SNOWFLAKE_DATABASE",
                "TAP_SNOWFLAKE_WAREHOUSE",
            ]
            if os.getenv(env) is None
        ]

        if missing_envs:
            raise ValueError(f"Missing environment variables: {missing_envs}")

        ensure_amplitude_test_tables()