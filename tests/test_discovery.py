
from pprint import pprint

import unittest
import tap_amplitude
import os
import pdb

from singer import get_logger, metadata
from utils import get_test_connection, ensure_test_table


LOGGER = get_logger()


class TestEventsTable(unittest.TestCase):
    table_name = "TEST_EVENTS_TABLE"
    schema_name = "PUBLIC"
    key_property = "UUID"
    replication_key = "SERVER_UPLOAD_TIME"


    def setup(self):
        # UUID and SERVER_UPLOAD_TIME are required: discovery only emits events
        # tables that expose a usable primary key and replication column.
        table_spec = { "columns": [
                       { "name": "UUID", "type": "STRING" },
                       { "name": "SERVER_UPLOAD_TIME", "type": "TIMESTAMP" },
                       { "name": "string", "type": "STRING" },
                       { "name": "integer", "type": "INTEGER" },
                       { "name": "time_created", "type": "TIMESTAMP" },
                       { "name": "object", "type": "VARIANT" },
                       { "name": "boolean", "type": "BOOLEAN" },
                       { "name": "number", "type": "NUMBER" },
                       { "name": "date_created", "type": "DATE" }
                     ],
                       "schema": TestEventsTable.schema_name,
                       "name": TestEventsTable.table_name }
        con = get_test_connection()
        ensure_test_table(con, table_spec)


    def test_catalog(self):
        con = get_test_connection()
        catalog = tap_amplitude.discover_catalog(con).to_dict()

        test_streams = [s for s in catalog['streams'] if s['tap_stream_id'] == "{}-{}".format(TestEventsTable.schema_name, TestEventsTable.table_name)]

        # Is there one stream found with same name?
        self.assertEqual(len(test_streams), 1)

        stream_dict = test_streams[0]
        self.assertEqual(TestEventsTable.table_name, stream_dict.get('table_name'))
        self.assertEqual("{}-{}".format(TestEventsTable.schema_name, TestEventsTable.table_name), stream_dict.get('stream'))

        # Check primary key is "UUID".
        mdata = metadata.to_map(stream_dict['metadata'])
        stream_metadata = mdata.get((), {})
        key_properties = stream_metadata.get('table-key-properties', [])
        self.assertEqual(TestEventsTable.key_property, key_properties[0])

        # Check the stream is INCREMENTAL on SERVER_UPLOAD_TIME.
        self.assertEqual(TestEventsTable.replication_key, stream_dict.get('replication_key'))
        self.assertEqual("INCREMENTAL", stream_dict.get('replication_method'))
        self.assertEqual("INCREMENTAL", stream_metadata.get('forced-replication-method'))
        self.assertEqual([TestEventsTable.replication_key], stream_metadata.get('valid-replication-keys'))

        # Check metadata.



class TestMergeTable(unittest.TestCase):
    table_name = 'TEST_MERGE_TABLE'
    schema_name = 'PUBLIC'
    replication_key = 'MERGE_EVENT_TIME'


    def setup(self):
        # MERGE_EVENT_TIME is required: discovery only emits merge tables that
        # expose a usable replication column.
        table_spec = { "columns": [
                       { "name": "MERGE_EVENT_TIME", "type": "TIMESTAMP" },
                       { "name": "string", "type": "STRING" },
                       { "name": "integer", "type": "INTEGER" },
                       { "name": "time_created", "type": "TIMESTAMP" },
                       { "name": "object", "type": "VARIANT" },
                       { "name": "boolean", "type": "BOOLEAN" },
                       { "name": "number", "type": "NUMBER" },
                       { "name": "date_created", "type": "DATE" }
                     ],
                       "schema": TestMergeTable.schema_name,
                       "name": TestMergeTable.table_name }
        con = get_test_connection()
        ensure_test_table(con, table_spec)


    def test_catalog(self):
        con = get_test_connection()
        catalog = tap_amplitude.discover_catalog(con).to_dict()

        test_streams = [s for s in catalog['streams'] if s['tap_stream_id'] == "{}-{}".format(TestMergeTable.schema_name, TestMergeTable.table_name)]

        # Is there one stream found with same name?
        self.assertEqual(len(test_streams), 1)

        # Check table_stream and stream name.
        stream_dict = test_streams[0]
        self.assertEqual(TestMergeTable.table_name, stream_dict.get('table_name'))
        self.assertEqual("{}-{}".format(TestMergeTable.schema_name, TestMergeTable.table_name), stream_dict.get('stream'))

        # Check that merge tables have no primary key.
        mdata = metadata.to_map(stream_dict['metadata'])
        stream_metadata = mdata.get((), {})
        key_properties = stream_metadata.get('table-key-properties', [])
        self.assertEqual([], key_properties)

        # Check the stream is INCREMENTAL on MERGE_EVENT_TIME.
        self.assertEqual(TestMergeTable.replication_key, stream_dict.get('replication_key'))
        self.assertEqual("INCREMENTAL", stream_dict.get('replication_method'))
        self.assertEqual("INCREMENTAL", stream_metadata.get('forced-replication-method'))
        self.assertEqual([TestMergeTable.replication_key], stream_metadata.get('valid-replication-keys'))

        # Check metadata.


class TestTableWithoutReplicationKey(unittest.TestCase):
    """Merge tables without MERGE_EVENT_TIME cannot be replicated incrementally."""
    table_name = 'TEST_MERGE_TABLE_NO_REPLICATION_KEY'
    schema_name = 'PUBLIC'


    def setup(self):
        table_spec = { "columns": [
                       { "name": "MERGE_ID", "type": "STRING" },
                       { "name": "string", "type": "STRING" },
                       { "name": "time_created", "type": "TIMESTAMP" }
                     ],
                       "schema": TestTableWithoutReplicationKey.schema_name,
                       "name": TestTableWithoutReplicationKey.table_name }
        con = get_test_connection()
        ensure_test_table(con, table_spec)


    def test_catalog(self):
        con = get_test_connection()
        catalog = tap_amplitude.discover_catalog(con).to_dict()

        tap_stream_id = "{}-{}".format(TestTableWithoutReplicationKey.schema_name,
                                       TestTableWithoutReplicationKey.table_name)
        test_streams = [s for s in catalog['streams'] if s['tap_stream_id'] == tap_stream_id]

        # The tap only supports INCREMENTAL replication, so a table without a
        # usable replication column must be excluded from the catalog.
        self.assertEqual(len(test_streams), 0)



test1 = TestEventsTable()
test1.setup()
test1.test_catalog()
test2 = TestMergeTable()
test2.setup()
test2.test_catalog()
test3 = TestTableWithoutReplicationKey()
test3.setup()
test3.test_catalog()




