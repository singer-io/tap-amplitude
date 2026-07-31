import datetime
import unittest
from unittest import mock

from singer.catalog import CatalogEntry, Schema

import tap_amplitude.sync_strategies.incremental as incremental


class _DummyTransformer:
    def transform(self, rec, *_args, **_kwargs):
        return rec


class _DummyTransformerContext:
    def __enter__(self):
        return _DummyTransformer()

    def __exit__(self, exc_type, exc, tb):
        return False


class _DummyCounter:
    def __init__(self):
        self.tags = {}
        self.value = 0

    def increment(self):
        self.value += 1


class _DummyCounterContext:
    def __init__(self, counter):
        self._counter = counter

    def __enter__(self):
        return self._counter

    def __exit__(self, exc_type, exc, tb):
        return False


class _FakeCursor:
    def __init__(self, rows):
        self._rows = list(rows)
        self.last_sql = None

    def execute(self, sql):
        self.last_sql = sql

    def fetchone(self):
        if not self._rows:
            return None
        return self._rows.pop(0)


class _FakeConnection:
    def __init__(self, cursor):
        self._cursor = cursor

    def cursor(self):
        return self._cursor


def _catalog_entry(metadata_list, replication_key="SERVER_UPLOAD_TIME"):
    schema = Schema.from_dict(
        {
            "type": "object",
            "properties": {
                "UUID": {"type": ["null", "string"]},
                "SERVER_UPLOAD_TIME": {
                    "type": ["null", "string"],
                    "format": "date-time",
                },
            },
        }
    )
    return CatalogEntry(
        stream="events",
        tap_stream_id="PUBLIC-events",
        schema=schema,
        metadata=metadata_list,
        replication_key=replication_key,
        replication_method="INCREMENTAL",
    )


class TestIncremental(unittest.TestCase):
    def test_get_selected_columns_includes_automatic_and_selected(self):
        metadata_list = [
            {"breadcrumb": ["properties", "UUID"], "metadata": {"selected": True}},
            {"breadcrumb": ["properties", "SERVER_UPLOAD_TIME"], "metadata": {"inclusion": "automatic"}},
            {"breadcrumb": ["properties", "IGNORED"], "metadata": {}},
        ]
        entry = _catalog_entry(metadata_list)
        columns = ["UUID", "SERVER_UPLOAD_TIME", "IGNORED"]

        result = incremental.get_selected_columns(entry, columns)

        self.assertEqual(["UUID", "SERVER_UPLOAD_TIME"], result)

    def test_generate_select_sql(self):
        sql = incremental.generate_select_sql("PUBLIC.events", ["UUID", "SERVER_UPLOAD_TIME"])
        self.assertIn("SELECT UUID,SERVER_UPLOAD_TIME", sql)
        self.assertIn("FROM PUBLIC.events", sql)

    def test_generate_select_sql_raises_for_empty_selection(self):
        with self.assertRaises(ValueError):
            incremental.generate_select_sql("PUBLIC.events", [])

    def test_process_row(self):
        row = ("abc", 1)
        result = incremental.process_row(row, ["a", "b"])
        self.assertEqual({"a": "abc", "b": 1}, result)

    @mock.patch("tap_amplitude.sync_strategies.incremental.singer.write_state")
    @mock.patch("tap_amplitude.sync_strategies.incremental.singer.write_bookmark")
    @mock.patch("tap_amplitude.sync_strategies.incremental.singer.write_record")
    @mock.patch("tap_amplitude.sync_strategies.incremental.pendulum.parse", return_value="PARSED")
    @mock.patch("tap_amplitude.sync_strategies.incremental.metrics.record_counter")
    @mock.patch("tap_amplitude.sync_strategies.incremental.Transformer", return_value=_DummyTransformerContext())
    def test_sync_table_existing_bookmark_datetime_and_state_every_100(
        self,
        _mock_transformer,
        mock_record_counter,
        _mock_parse,
        mock_write_record,
        mock_write_bookmark,
        mock_write_state,
    ):
        metadata_list = [
            {"breadcrumb": ["properties", "UUID"], "metadata": {"selected": True}},
            {"breadcrumb": ["properties", "SERVER_UPLOAD_TIME"], "metadata": {"inclusion": "automatic"}},
        ]
        entry = _catalog_entry(metadata_list)
        state = {
            "bookmarks": {
                "PUBLIC-events": {
                    "SERVER_UPLOAD_TIME": "2026-01-01T00:00:00Z",
                }
            }
        }

        rows = []
        for index in range(100):
            rows.append((f"id-{index}", datetime.datetime(2026, 1, 1, 0, 0, 0)))
        cursor = _FakeCursor(rows)
        connection = _FakeConnection(cursor)

        counter = _DummyCounter()
        mock_record_counter.return_value = _DummyCounterContext(counter)

        result = incremental.sync_table(
            connection,
            entry,
            state,
            ["UUID", "SERVER_UPLOAD_TIME"],
        )

        self.assertEqual(100, result)
        self.assertIn("WHERE SERVER_UPLOAD_TIME >= 'PARSED'", cursor.last_sql)
        self.assertIn("ORDER BY SERVER_UPLOAD_TIME ASC", cursor.last_sql)
        self.assertEqual(100, mock_write_record.call_count)
        self.assertEqual(100, mock_write_bookmark.call_count)
        self.assertGreaterEqual(mock_write_state.call_count, 2)

    @mock.patch("tap_amplitude.sync_strategies.incremental.singer.write_state")
    @mock.patch("tap_amplitude.sync_strategies.incremental.singer.write_bookmark")
    @mock.patch("tap_amplitude.sync_strategies.incremental.metrics.record_counter")
    @mock.patch("tap_amplitude.sync_strategies.incremental.Transformer", return_value=_DummyTransformerContext())
    def test_sync_table_initial_bookmark_and_order_by_without_where(
        self,
        _mock_transformer,
        mock_record_counter,
        mock_write_bookmark,
        _mock_write_state,
    ):
        metadata_list = [
            {"breadcrumb": ["properties", "UUID"], "metadata": {"selected": True}},
            {"breadcrumb": ["properties", "SERVER_UPLOAD_TIME"], "metadata": {"inclusion": "automatic"}},
        ]
        entry = _catalog_entry(metadata_list)
        state = {}
        cursor = _FakeCursor([])
        connection = _FakeConnection(cursor)

        counter = _DummyCounter()
        mock_record_counter.return_value = _DummyCounterContext(counter)

        result = incremental.sync_table(
            connection,
            entry,
            state,
            ["UUID", "SERVER_UPLOAD_TIME"],
        )

        self.assertEqual(0, result)
        mock_write_bookmark.assert_any_call(state, "PUBLIC-events", "SERVER_UPLOAD_TIME", None)
        self.assertIn("ORDER BY SERVER_UPLOAD_TIME ASC", cursor.last_sql)
        self.assertNotIn("WHERE", cursor.last_sql)
