
from pprint import pprint

import unittest
import tap_amplitude
import os
import pdb

from singer import get_logger, metadata
from utils import get_test_connection, ensure_test_table


LOGGER = get_logger()


class TestTable(unittest.TestCase):
    table_name = 'TEST_TABLE'
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
                       "schema": TestTable.schema_name,
                       "name": TestTable.table_name }
        con = get_test_connection()
        ensure_test_table(con, table_spec)


    def test_catalog(self):
        con = get_test_connection()
        catalog = tap_amplitude.discover_catalog(con).to_dict()

        test_streams = [s for s in catalog['streams'] if s['tap_stream_id'] == "{}-{}".format(TestTable.schema_name, TestTable.table_name)]

        # Is there one stream found with same name?
        self.assertEqual(len(test_streams), 1)

        stream_dict = test_streams[0]
        self.assertEqual(TestTable.table_name, stream_dict.get('table_name'))
        self.assertEqual(TestTable.table_name, stream_dict.get('stream'))


        # Check primary key is "UUID" for table that has "events"
        # Check no key for table that has "merge"
        # Check metadata.



test1 = TestTable()
test1.setup()
test1.test_catalog()


