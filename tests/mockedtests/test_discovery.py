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
        self.assertIn("events_table", streams)
        self.assertIn("merge_table", streams)
        self.assertEqual("SERVER_UPLOAD_TIME", streams["events_table"].replication_key)
        self.assertEqual("MERGE_EVENT_TIME", streams["merge_table"].replication_key)
        self.assertIn("information_schema.columns", connection._cursor.executed_sql)
