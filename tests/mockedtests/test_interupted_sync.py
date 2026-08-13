import datetime
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


class _Cursor:
    def __init__(self, rows):
        self._rows = list(rows)

    def execute(self, _sql):
        return None

    def fetchone(self):
        if not self._rows:
            return None
        return self._rows.pop(0)


class _Connection:
    def __init__(self, rows):
        self._cursor = _Cursor(rows)

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


class TestInterruptedSync(unittest.TestCase):
    @mock.patch("tap_amplitude.sync_strategies.incremental.singer.write_state")
    @mock.patch("tap_amplitude.sync_strategies.incremental.singer.write_bookmark")
    @mock.patch("tap_amplitude.sync_strategies.incremental.singer.write_record")
    @mock.patch("tap_amplitude.sync_strategies.incremental.metrics.record_counter")
    @mock.patch("tap_amplitude.sync_strategies.incremental.Transformer", return_value=_DummyTransformerContext())
    def test_interupted_sync(self, _mock_transformer, mock_record_counter, mock_write_record, mock_write_bookmark, _mock_write_state):
        counter = _DummyCounter()
        mock_record_counter.return_value = _DummyCounterContext(counter)

        rows = [
            ("id-1", datetime.datetime(2026, 1, 1, 0, 0, 0)),
            ("id-2", datetime.datetime(2026, 1, 2, 0, 0, 0)),
        ]
        connection = _Connection(rows)
        state = {"bookmarks": {"PUBLIC-events": {"SERVER_UPLOAD_TIME": None}}}

        mock_write_record.side_effect = [None, RuntimeError("interrupt")]

        with self.assertRaises(RuntimeError):
            incremental.sync_table(connection, _entry(), state, ["UUID", "SERVER_UPLOAD_TIME"])

        self.assertGreaterEqual(mock_write_bookmark.call_count, 1)
