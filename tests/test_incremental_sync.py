import unittest
import os
from singer import get_logger, metadata
from utils import get_test_connection, ensure_test_table
from tap_amplitude import discover_catalog  # Correct import

LOGGER = get_logger()

class TestIncrementalSync(unittest.TestCase):
    table_name = "TEST_EVENTS_TABLE"
    schema_name = "PUBLIC"
    replication_key = "TIME_CREATED"
    key_property = "UUID"

    @classmethod
    def setUpClass(cls):
        table_spec = {
            "columns": [
                {"name": "UUID", "type": "STRING"},
                {"name": "STRING", "type": "STRING"},
                {"name": "INTEGER", "type": "INTEGER"},
                {"name": "NUMBER", "type": "NUMBER"},
                {"name": "TIME_CREATED", "type": "TIMESTAMP"},
                {"name": "DATE_CREATED", "type": "DATE"},
                {"name": "OBJECT", "type": "VARIANT"},
                {"name": "BOOLEAN", "type": "BOOLEAN"}
            ],
            "schema": cls.schema_name,
            "name": cls.table_name
        }
        con = get_test_connection()
        ensure_test_table(con, table_spec)

    def test_discover_includes_replication_key(self):
        con = get_test_connection()
        catalog = discover_catalog(con).to_dict()

        stream_id = f"{self.schema_name}-{self.table_name}"
        test_streams = [s for s in catalog["streams"] if s["tap_stream_id"] == stream_id]
        self.assertEqual(len(test_streams), 1)

        stream = test_streams[0]
        mdata = metadata.to_map(stream["metadata"])
        valid_replication_keys = mdata.get((), {}).get("valid-replication-keys", [])
        self.assertIn(self.replication_key, valid_replication_keys)

    def test_replication_key_in_schema(self):
        con = get_test_connection()
        catalog = discover_catalog(con).to_dict()

        stream_id = f"{self.schema_name}-{self.table_name}"
        stream = next(s for s in catalog["streams"] if s["tap_stream_id"] == stream_id)
        self.assertIn(self.replication_key, stream["schema"]["properties"])

if __name__ == "__main__":
    unittest.main()
