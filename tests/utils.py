from pprint import pprint
import os
import singer

from singer import get_logger
from tap_amplitude.connection import connect_with_backoff

LOGGER = get_logger()


def get_test_snowflake_config():
    required_env_vars = [
        'TAP_SNOWFLAKE_USERNAME',
        'TAP_SNOWFLAKE_PASSWORD',
        'TAP_SNOWFLAKE_ACCOUNT',
        'TAP_SNOWFLAKE_DATABASE',
        'TAP_SNOWFLAKE_WAREHOUSE'
    ]
    missing = [var for var in required_env_vars if not os.getenv(var)]
    if missing:
        raise Exception(f"Missing environment variables: {', '.join(missing)}")

    return {
        'username': os.getenv('TAP_SNOWFLAKE_USERNAME'),
        'password': os.getenv('TAP_SNOWFLAKE_PASSWORD'),
        'account': os.getenv('TAP_SNOWFLAKE_ACCOUNT'),
        'database': os.getenv('TAP_SNOWFLAKE_DATABASE'),
        'warehouse': os.getenv('TAP_SNOWFLAKE_WAREHOUSE'),
    }


def get_test_connection():
    if os.getenv("CIRCLECI"):
        LOGGER.info("Running in CircleCI - using mock Snowflake connection")

        class DummyConnection:
            def cursor(self):
                class DummyCursor:
                    def __enter__(self): return self
                    def __exit__(self, exc_type, exc_val, exc_tb): pass
                    def execute(self, sql): LOGGER.info("Mock execute: %s", sql)
                return DummyCursor()

        return DummyConnection()

    else:
        config = get_test_snowflake_config()
        return connect_with_backoff(config)


def build_col_sql(col):
    return f"{col['name']} {col['type']}"


# Renamed function to avoid nose auto-discovery
def _ensure_test_table(con, table_spec):
    col_sql = ", ".join(build_col_sql(c) for c in table_spec['columns'])
    with con.cursor() as cursor:
        sql = f"""
            CREATE OR REPLACE TRANSIENT TABLE {table_spec['schema']}.{table_spec['name']} ({col_sql})
        """
        LOGGER.info("Create table SQL: %s", sql)
        cursor.execute(sql)


def set_replication_method_and_key(con, method_and_key):
    return
