
from pprint import pprint

import os
import backoff
import snowflake.connector
import singer

from singer import get_logger


LOGGER = get_logger()
TEST_SCHEMA_NAME = "PUBLIC"
TEST_EVENTS_TABLE = "TAP_TEST_EVENTS"
TEST_MERGE_TABLE = "TAP_TEST_MERGE_EVENTS"


@backoff.on_exception(
    backoff.expo,
    (snowflake.connector.Error,),
    max_tries=5,
    factor=2,
)
def connect_with_backoff(config):
    return snowflake.connector.connect(
        user=config['username'],
        password=config['password'],
        account=config['account'],
        database=config['database'],
        warehouse=config['warehouse'],
    )


def get_test_snowflake_config():
    missing_envs = [x for x in [os.getenv('TAP_SNOWFLAKE_USERNAME'),
                                os.getenv('TAP_SNOWFLAKE_PASSWORD'),
                                os.getenv('TAP_SNOWFLAKE_ACCOUNT'),
                                os.getenv('TAP_SNOWFLAKE_DATABASE'),
                                os.getenv('TAP_SNOWFLAKE_WAREHOUSE')] if x == None]
    if len(missing_envs) != 0:
        #pylint: disable=line-too-long
        raise Exception("set TAP_SNOWFLAKE_USERNAME, TAP_SNOWFLAKE_PASSWORD, TAP_SNOWFLAKE_ACCOUNT, TAP_SNOWFLAKE_DATABASE, TAP_SNOWFLAKE_WAREHOUSE")

    config = {}
    config['username'] = os.environ.get('TAP_SNOWFLAKE_USERNAME')
    config['password'] = os.environ.get('TAP_SNOWFLAKE_PASSWORD')
    config['account'] = os.environ.get('TAP_SNOWFLAKE_ACCOUNT')
    config['database'] = os.environ.get('TAP_SNOWFLAKE_DATABASE')
    config['warehouse'] = os.environ.get('TAP_SNOWFLAKE_WAREHOUSE')
    return config


def get_test_connection():
    config = get_test_snowflake_config()
    return connect_with_backoff(config)


def build_col_sql(col):
    return "{} {}".format(col['name'], col['type'])


def ensure_test_table(con, table_spec):
    col_sql = map(lambda c: build_col_sql(c), table_spec['columns'])
    with con.cursor() as cursor:
        sql = """
            CREATE OR REPLACE TRANSIENT TABLE {}.{} ({})
            """.format(table_spec['schema'], table_spec['name'], ",".join(col_sql))
        LOGGER.info("Create table sql: %s", sql)
        cursor.execute(sql)


def insert_rows(con, schema_name, table_name, rows):
    if not rows:
        return

    with con.cursor() as cursor:
        cursor.execute("DELETE FROM {}.{}".format(schema_name, table_name))
        for row in rows:
            cursor.execute(row)


def ensure_amplitude_test_tables():
    con = get_test_connection()

    event_table_spec = {
        "schema": TEST_SCHEMA_NAME,
        "name": TEST_EVENTS_TABLE,
        "columns": [
            {"name": "UUID", "type": "STRING"},
            {"name": "SERVER_UPLOAD_TIME", "type": "TIMESTAMP_NTZ"},
            {"name": "EVENT_TYPE", "type": "STRING"},
            {"name": "EVENT_PROPERTIES", "type": "VARIANT"},
            {"name": "IS_ACTIVE", "type": "BOOLEAN"},
            {"name": "REVENUE", "type": "NUMBER"},
        ],
    }
    merge_table_spec = {
        "schema": TEST_SCHEMA_NAME,
        "name": TEST_MERGE_TABLE,
        "columns": [
            {"name": "MERGE_ID", "type": "STRING"},
            {"name": "MERGE_EVENT_TIME", "type": "TIMESTAMP_NTZ"},
            {"name": "AMPLITUDE_ID", "type": "STRING"},
            {"name": "MERGED_USER_ID", "type": "STRING"},
            {"name": "MERGE_PAYLOAD", "type": "VARIANT"},
            {"name": "IS_MERGED", "type": "BOOLEAN"},
        ],
    }

    ensure_test_table(con, event_table_spec)
    ensure_test_table(con, merge_table_spec)

    insert_rows(
        con,
        TEST_SCHEMA_NAME,
        TEST_EVENTS_TABLE,
        [
            """
            INSERT INTO PUBLIC.TAP_TEST_EVENTS
            (UUID, SERVER_UPLOAD_TIME, EVENT_TYPE, EVENT_PROPERTIES, IS_ACTIVE, REVENUE)
            SELECT
                'event-1',
                TO_TIMESTAMP_NTZ('2026-01-01T00:00:00'),
                'session_start',
                PARSE_JSON('{"source":"tests","plan":"free"}'),
                TRUE,
                10
            """,
            """
            INSERT INTO PUBLIC.TAP_TEST_EVENTS
            (UUID, SERVER_UPLOAD_TIME, EVENT_TYPE, EVENT_PROPERTIES, IS_ACTIVE, REVENUE)
            SELECT
                'event-2',
                TO_TIMESTAMP_NTZ('2026-01-02T00:00:00'),
                'purchase',
                PARSE_JSON('{"source":"tests","plan":"pro"}'),
                FALSE,
                25
            """,
        ],
    )
    insert_rows(
        con,
        TEST_SCHEMA_NAME,
        TEST_MERGE_TABLE,
        [
            """
            INSERT INTO PUBLIC.TAP_TEST_MERGE_EVENTS
            (MERGE_ID, MERGE_EVENT_TIME, AMPLITUDE_ID, MERGED_USER_ID, MERGE_PAYLOAD, IS_MERGED)
            SELECT
                'merge-1',
                TO_TIMESTAMP_NTZ('2026-01-03T00:00:00'),
                'amp-1',
                'user-1',
                PARSE_JSON('{"reason":"identity_merge"}'),
                TRUE
            """,
            """
            INSERT INTO PUBLIC.TAP_TEST_MERGE_EVENTS
            (MERGE_ID, MERGE_EVENT_TIME, AMPLITUDE_ID, MERGED_USER_ID, MERGE_PAYLOAD, IS_MERGED)
            SELECT
                'merge-2',
                TO_TIMESTAMP_NTZ('2026-01-04T00:00:00'),
                'amp-2',
                'user-2',
                PARSE_JSON('{"reason":"backfill"}'),
                FALSE
            """,
        ],
    )


def set_replication_method_and_key(con, method_and_key):
    # Create Catalog with `replication_method` and `replication_key`.
    return
