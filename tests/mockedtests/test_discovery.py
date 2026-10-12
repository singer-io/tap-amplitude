import unittest

import tap_amplitude


class _DiscoverCursor:
    def __init__(self, records):
        self._records = list(records)
        self.executed_sql = None

    def execute(self, sql):
        self.executed_sql = sql

    def __iter__(self):
        return iter(self._records)


class _Connection:
    def __init__(self, records):
        self._cursor = _DiscoverCursor(records)

    def cursor(self):
        return self._cursor


class TestDiscovery(unittest.TestCase):
    def test_discovery(self):
        records = [
            ("PUBLIC", "events_table", "UUID", "STRING", None, None, None),
            ("PUBLIC", "events_table", "SERVER_UPLOAD_TIME", "TIMESTAMP_NTZ", None, None, None),
            ("PUBLIC", "merge_table", "MERGE_EVENT_TIME", "TIMESTAMP_NTZ", None, None, None),
        ]
        connection = _Connection(records)

        catalog = tap_amplitude.discover_catalog(connection)
        streams = {entry.stream: entry for entry in catalog.streams}

        self.assertEqual(2, len(catalog.streams))
        self.assertIn("PUBLIC-events_table", streams)
        self.assertIn("PUBLIC-merge_table", streams)
        
        # Events table should have UUID as key and SERVER_UPLOAD_TIME as replication key
        self.assertEqual(["UUID"], streams["PUBLIC-events_table"].key_properties)
        self.assertEqual("SERVER_UPLOAD_TIME", streams["PUBLIC-events_table"].replication_key)
        
        # Merge table should have no primary key and MERGE_EVENT_TIME as replication key
        self.assertEqual([], streams["PUBLIC-merge_table"].key_properties)
        self.assertEqual("MERGE_EVENT_TIME", streams["PUBLIC-merge_table"].replication_key)
        
        self.assertIn("information_schema.columns", connection._cursor.executed_sql)

    def test_discovery_merge_table_with_merge_id(self):
        """Test merge tables that have both MERGE_ID and MERGE_EVENT_TIME columns."""
        records = [
            ("PUBLIC", "merge_events", "MERGE_ID", "STRING", None, None, None),
            ("PUBLIC", "merge_events", "MERGE_EVENT_TIME", "TIMESTAMP_NTZ", None, None, None),
            ("PUBLIC", "merge_events", "USER_ID", "STRING", None, None, None),
        ]
        connection = _Connection(records)

        catalog = tap_amplitude.discover_catalog(connection)
        streams = {entry.stream: entry for entry in catalog.streams}

        self.assertEqual(1, len(catalog.streams))
        self.assertIn("PUBLIC-merge_events", streams)
        
        # Merge table has no primary key
        self.assertEqual([], streams["PUBLIC-merge_events"].key_properties)
        self.assertEqual("MERGE_EVENT_TIME", streams["PUBLIC-merge_events"].replication_key)

    def test_discovery_events_table_without_expected_columns(self):
        """Events tables without SERVER_UPLOAD_TIME are excluded from the catalog."""
        records = [
            ("PUBLIC", "custom_events_table", "ID", "STRING", None, None, None),
            ("PUBLIC", "custom_events_table", "EVENT_TIME", "TIMESTAMP_NTZ", None, None, None),
        ]
        connection = _Connection(records)

        catalog = tap_amplitude.discover_catalog(connection)
        streams = {entry.stream: entry for entry in catalog.streams}

        # The tap only supports INCREMENTAL replication. A table with neither
        # UUID nor SERVER_UPLOAD_TIME must not be discoverable, otherwise the
        # catalog would advertise nonexistent fields and incremental sync would
        # build an ORDER BY against a column Snowflake cannot resolve.
        self.assertEqual(0, len(catalog.streams))
        self.assertNotIn("PUBLIC-custom_events_table", streams)

    def test_discovery_never_advertises_columns_missing_from_the_table(self):
        """Discovered streams must only reference columns that actually exist."""
        records = [
            ("PUBLIC", "custom_events_table", "ID", "STRING", None, None, None),
            ("PUBLIC", "custom_events_table", "EVENT_TIME", "TIMESTAMP_NTZ", None, None, None),
            ("PUBLIC", "events_table", "UUID", "STRING", None, None, None),
            ("PUBLIC", "events_table", "SERVER_UPLOAD_TIME", "TIMESTAMP_NTZ", None, None, None),
            ("PUBLIC", "merge_events", "MERGE_EVENT_TIME", "TIMESTAMP_NTZ", None, None, None),
            ("PUBLIC", "merge_no_time", "MERGE_ID", "STRING", None, None, None),
        ]
        connection = _Connection(records)

        catalog = tap_amplitude.discover_catalog(connection)

        self.assertEqual(
            {"PUBLIC-events_table", "PUBLIC-merge_events"},
            {entry.stream for entry in catalog.streams}
        )

        for entry in catalog.streams:
            schema_properties = set(entry.schema.properties.keys())
            self.assertIn(entry.replication_key, schema_properties)
            for key in entry.key_properties:
                self.assertIn(key, schema_properties)

    def test_discovery_events_table_without_uuid_column(self):
        """Events tables with SERVER_UPLOAD_TIME but no UUID are discovered without a key."""
        records = [
            ("PUBLIC", "custom_events_table", "ID", "STRING", None, None, None),
            ("PUBLIC", "custom_events_table", "SERVER_UPLOAD_TIME", "TIMESTAMP_NTZ", None, None, None),
        ]
        connection = _Connection(records)

        catalog = tap_amplitude.discover_catalog(connection)
        streams = {entry.stream: entry for entry in catalog.streams}

        self.assertEqual(1, len(catalog.streams))
        entry = streams["PUBLIC-custom_events_table"]
        self.assertEqual("SERVER_UPLOAD_TIME", entry.replication_key)
        self.assertEqual("INCREMENTAL", entry.replication_method)
        self.assertEqual([], entry.key_properties)

