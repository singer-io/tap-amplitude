import unittest
from singer import get_logger, metadata
from utils import get_test_connection, _ensure_test_table

from tap_amplitude import discover_catalog

LOGGER = get_logger()

class TestEventsTable(unittest.TestCase):
    table_name = "TEST_EVENTS_TABLE"
    schema_name = "PUBLIC"
    key_property = "UUID"

    @classmethod
    def setUpClass(cls):
        table_spec = {
            "columns": [
                {"name": "UUID", "type": "STRING"},
                {"name": "string", "type": "STRING"},
                {"name": "integer", "type": "INTEGER"},
                {"name": "time_created", "type": "TIMESTAMP"},
            ],
            "schema": cls.schema_name,
            "name": cls.table_name,
        }
        con = get_test_connection()
        _ensure_test_table(con, table_spec)  # Update

    def test_catalog_has_correct_stream(self):
        con = get_test_connection()
        catalog = discover_catalog(con).to_dict()

        test_streams = [
            s for s in catalog["streams"]
            if s["tap_stream_id"] == f"{self.schema_name}-{self.table_name}"
        ]

        self.assertEqual(len(test_streams), 1)

    def test_primary_key_is_uuid(self):
        con = get_test_connection()
        catalog = discover_catalog(con).to_dict()
        stream_dict = next(
            s for s in catalog["streams"]
            if s["tap_stream_id"] == f"{self.schema_name}-{self.table_name}"
        )
        mdata = metadata.to_map(stream_dict["metadata"])
        key_properties = mdata.get((), {}).get("table-key-properties", [])
        self.assertEqual(self.key_property, key_properties[0])


if __name__ == "__main__":
    unittest.main()
