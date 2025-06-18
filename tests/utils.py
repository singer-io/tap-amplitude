
from pprint import pprint

import os
import singer
from nose.tools import nottest

from singer import get_logger
from tap_amplitude.connection import connect_with_backoff


LOGGER = get_logger()


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


@nottest
def ensure_test_table(con, table_spec):
    col_sql = map(lambda c: build_col_sql(c), table_spec['columns'])
    with con.cursor() as cursor:
        sql = """
            CREATE OR REPLACE TRANSIENT TABLE {}.{} ({})
            """.format(table_spec['schema'], table_spec['name'], ",".join(col_sql))
        LOGGER.info("Create table sql: %s", sql)
        cursor.execute(sql)


def set_replication_method_and_key(con, method_and_key):
    # Create Catalog with `replication_method` and `replication_key`.
    return
