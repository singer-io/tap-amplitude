import datetime
import re
import unittest
from unittest import mock

from singer.catalog import CatalogEntry
from singer.schema import Schema

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


class _FilteringCursor:
    def __init__(self, rows):
        self._source_rows = list(rows)
        self._rows = []

    def execute(self, sql):
        self._rows = list(self._source_rows)
        match = re.search(r"WHERE\s+SERVER_UPLOAD_TIME\s+>=\s+'([^']+)'", sql)
        if match:
            threshold = datetime.datetime.fromisoformat(match.group(1))
            self._rows = [row for row in self._rows if row[1] >= threshold]

    def fetchone(self):
        if not self._rows:
            return None
        return self._rows.pop(0)


class _Connection:
    def __init__(self, rows):
        self._cursor = _FilteringCursor(rows)

    def cursor(self):
        return self._cursor


def _entry():
    schema = Schema.from_dict(
        {
            "type": "object",
            "properties": {
                "UUID": {"type": ["null", "string"]},
                "SERVER_UPLOAD_TIME": {"type": ["null", "string"], "format": "date-time"},
            },
        }
    )
    return CatalogEntry(
        stream="events",
        tap_stream_id="PUBLIC-events",
        schema=schema,
        metadata=[
            {"breadcrumb": [], "metadata": {"selected": True}},
            {"breadcrumb": ["properties", "UUID"], "metadata": {"selected": True}},
            {"breadcrumb": ["properties", "SERVER_UPLOAD_TIME"], "metadata": {"inclusion": "automatic"}},
        ],
        replication_key="SERVER_UPLOAD_TIME",
        replication_method="INCREMENTAL",
    )


class TestBookmark(unittest.TestCase):
    @mock.patch("tap_amplitude.sync_strategies.incremental.singer.write_state")
    @mock.patch("tap_amplitude.sync_strategies.incremental.singer.write_bookmark")
    @mock.patch("tap_amplitude.sync_strategies.incremental.singer.write_record")
    @mock.patch("tap_amplitude.sync_strategies.incremental.metrics.record_counter")
    @mock.patch("tap_amplitude.sync_strategies.incremental.Transformer", return_value=_DummyTransformerContext())
    @mock.patch("tap_amplitude.sync_strategies.incremental.pendulum.parse")
    def test_bookmark(self, mock_parse, _mock_transformer, mock_record_counter, mock_write_record, mock_write_bookmark, _mock_write_state):
        mock_parse.return_value = datetime.datetime(2026, 1, 2, 0, 0, 0)
        counter = _DummyCounter()
        mock_record_counter.return_value = _DummyCounterContext(counter)

        rows = [
            ("id-old", datetime.datetime(2026, 1, 1, 0, 0, 0)),
            ("id-new", datetime.datetime(2026, 1, 3, 0, 0, 0)),
        ]
        connection = _Connection(rows)
        state = {"bookmarks": {"PUBLIC-events": {"SERVER_UPLOAD_TIME": "2026-01-02T00:00:00Z"}}}

        incremental.sync_table(connection, _entry(), state, ["UUID", "SERVER_UPLOAD_TIME"])

        self.assertEqual(1, mock_write_record.call_count)
        written = mock_write_record.call_args[0][1]
        self.assertEqual("id-new", written["UUID"])
        self.assertTrue(mock_write_bookmark.called)
