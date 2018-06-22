
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


    def setup(self):
        table_spec = { "columns": [
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
        self.assertEqual(TestEventsTable.table_name, stream_dict.get('stream'))

        # Check primary key is "UUID".
        mdata = metadata.to_map(stream_dict['metadata'])
        stream_metadata = mdata.get((), {})
        key_properties = stream_metadata.get('table-key-properties', [])
        self.assertEqual(TestEventsTable.key_property, key_properties[0])

        # Check metadata.



class TestMergeTable(unittest.TestCase):
    table_name = 'TEST_MERGE_TABLE'
    schema_name = 'PUBLIC'


    def setup(self):
        table_spec = { "columns": [
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
        self.assertEqual(TestMergeTable.table_name, stream_dict.get('stream'))

        # Check that there is no key property.
        mdata = metadata.to_map(stream_dict['metadata'])
        stream_metadata = mdata.get((), {})
        key_properties = stream_metadata.get('table-key-properties', [])
        self.assertEqual(len(key_properties), 0)

        # Check metadata.



test1 = TestEventsTable()
test1.setup()
test1.test_catalog()
test2 = TestMergeTable()
test2.setup()
test2.test_catalog()




