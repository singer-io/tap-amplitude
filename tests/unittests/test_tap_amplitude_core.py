import unittest
from unittest import mock

from singer.catalog import Catalog, CatalogEntry
from singer.schema import Schema

import tap_amplitude


class _FakeCursor:
    def __init__(self, records):
        self._records = records
        self.executed_sql = None

    def execute(self, sql):
        self.executed_sql = sql

    def __iter__(self):
        return iter(self._records)


class _FakeConnection:
    def __init__(self, records):
        self._cursor = _FakeCursor(records)

    def cursor(self):
        return self._cursor


class _DummyTimer:
    def __init__(self):
        self.tags = {}


class _DummyTimerContext:
    def __enter__(self):
        return _DummyTimer()

    def __exit__(self, exc_type, exc, tb):
        return False


def _entry(stream_name, selected=True, replication_key="SERVER_UPLOAD_TIME"):
    schema = Schema.from_dict(
        {
            "type": "object",
            "properties": {
                "UUID": {"type": ["null", "string"]},
                "SERVER_UPLOAD_TIME": {"type": ["null", "string"], "format": "date-time"},
            },
        }
    )
    stream_metadata = {"selected": selected, "table-key-properties": ["UUID"]}
    metadata = [
        {"breadcrumb": [], "metadata": stream_metadata},
        {"breadcrumb": ["properties", "UUID"], "metadata": {"selected": True}},
        {"breadcrumb": ["properties", "SERVER_UPLOAD_TIME"], "metadata": {"inclusion": "automatic"}},
    ]
    return CatalogEntry(
        stream=stream_name,
        tap_stream_id=f"PUBLIC-{stream_name}",
        schema=schema,
        metadata=metadata,
        replication_key=replication_key,
        replication_method="INCREMENTAL" if replication_key else "FULL_TABLE",
    )


class TestTapAmplitudeCore(unittest.TestCase):
    def test_schema_for_supported_and_unsupported_column_types(self):
        column_supported = tap_amplitude.Column("PUBLIC", "events", "time_created", "TIMESTAMP_NTZ", None, None, None)
        supported_schema = tap_amplitude.schema_for_column(column_supported)
        self.assertEqual("date-time", supported_schema.format)

        column_unknown = tap_amplitude.Column("PUBLIC", "events", "x", "BINARY", None, None, None)
        unsupported_schema = tap_amplitude.schema_for_column(column_unknown)
        self.assertEqual("unsupported", unsupported_schema.inclusion)
        self.assertIn("Unsupported column type binary", unsupported_schema.description)

    def test_create_column_metadata(self):
        columns = [
            tap_amplitude.Column("PUBLIC", "events", "UUID", "STRING", None, None, None),
            tap_amplitude.Column("PUBLIC", "events", "SERVER_UPLOAD_TIME", "TIMESTAMP_NTZ", None, None, None),
        ]

        mdata_list = tap_amplitude.create_column_metadata(columns)

        self.assertTrue(
            any(tuple(item["breadcrumb"]) == ("properties", "UUID") for item in mdata_list)
        )

    def test_discover_catalog_events_merge_and_full_table(self):
        records = [
            ("PUBLIC", "events_table", "UUID", "STRING", None, None, None),
            ("PUBLIC", "events_table", "SERVER_UPLOAD_TIME", "TIMESTAMP_NTZ", None, None, None),
            ("PUBLIC", "merge_table", "MERGE_EVENT_TIME", "TIMESTAMP_NTZ", None, None, None),
            ("PUBLIC", "AMPLITUDE_MERGE_EVENTS", "MERGE_EVENT_TIME", "TIMESTAMP_NTZ", None, None, None),
            ("PUBLIC", "AMPLITUDE_MERGE_EVENTS", "MERGE_ID", "STRING", None, None, None),
            ("PUBLIC", "other_table", "FIELD", "STRING", None, None, None),
        ]
        connection = _FakeConnection(records)

        catalog = tap_amplitude.discover_catalog(connection)

        self.assertEqual(4, len(catalog.streams))
        events_entry = next(s for s in catalog.streams if s.stream == "PUBLIC-events_table")
        merge_entry = next(s for s in catalog.streams if s.stream == "PUBLIC-merge_table")
        amplitude_merge_entry = next(s for s in catalog.streams if s.stream == "PUBLIC-AMPLITUDE_MERGE_EVENTS")
        other_entry = next(s for s in catalog.streams if s.stream == "PUBLIC-other_table")

        self.assertEqual("SERVER_UPLOAD_TIME", events_entry.replication_key)
        self.assertEqual("INCREMENTAL", events_entry.replication_method)
        self.assertEqual("MERGE_EVENT_TIME", merge_entry.replication_key)
        self.assertEqual("MERGE_EVENT_TIME", amplitude_merge_entry.replication_key)
        self.assertEqual(["UUID"], events_entry.key_properties)
# Merge tables have no primary key
        self.assertEqual([], merge_entry.key_properties)
        self.assertEqual([], amplitude_merge_entry.key_properties)

        amplitude_merge_md = {
            tuple(item["breadcrumb"]): item["metadata"]
            for item in amplitude_merge_entry.metadata
        }
        self.assertNotIn(("properties", "UUID"), amplitude_merge_md)
        self.assertNotIn(("properties", "SERVER_UPLOAD_TIME"), amplitude_merge_md)
        # Verify MERGE_EVENT_TIME is automatic for merge tables
        self.assertEqual("automatic", amplitude_merge_md.get(("properties", "MERGE_EVENT_TIME"), {}).get("inclusion"))
        self.assertEqual("FULL_TABLE", other_entry.replication_method)

    @mock.patch("tap_amplitude.discover_catalog")
    def test_do_discover(self, mock_discover_catalog):
        catalog = mock.MagicMock()
        mock_discover_catalog.return_value = catalog

        tap_amplitude.do_discover(mock.MagicMock())

        catalog.dump.assert_called_once_with()

    def test_get_key_properties_and_stream_selection(self):
        entry = _entry("events")
        keys = tap_amplitude.get_key_properties(entry)
        self.assertEqual(["UUID"], keys)

        self.assertTrue(tap_amplitude.stream_is_selected({(): {"selected": True}}))
        self.assertFalse(tap_amplitude.stream_is_selected({(): {}}))

    @mock.patch("tap_amplitude.sync_incremental.sync_table", return_value=[1, 2])
    @mock.patch("tap_amplitude.metrics.job_timer", return_value=_DummyTimerContext())
    @mock.patch("tap_amplitude.singer.write_schema")
    def test_do_sync_incremental_success(self, _mock_write_schema, _mock_job_timer, mock_sync_table):
        entry = _entry("events")
        rows = tap_amplitude.do_sync_incremental(mock.MagicMock(), entry, {}, ["UUID", "SERVER_UPLOAD_TIME"])
        self.assertEqual([1, 2], rows)
        mock_sync_table.assert_called_once()

    @mock.patch("tap_amplitude.sync_incremental.sync_table", return_value=None)
    @mock.patch("tap_amplitude.metrics.job_timer", return_value=_DummyTimerContext())
    @mock.patch("tap_amplitude.singer.write_schema")
    @mock.patch("tap_amplitude.LOGGER.warning")
    def test_do_sync_incremental_none_rows(self, mock_warning, _mock_write_schema, _mock_job_timer, _mock_sync_table):
        entry = _entry("events")
        rows = tap_amplitude.do_sync_incremental(mock.MagicMock(), entry, {}, ["UUID", "SERVER_UPLOAD_TIME"])
        self.assertEqual([], rows)
        mock_warning.assert_called_once()

    def test_do_sync_incremental_raises_without_replication_key(self):
        entry = _entry("events", replication_key=None)
        with self.assertRaises(Exception):
            tap_amplitude.do_sync_incremental(mock.MagicMock(), entry, {}, ["UUID"])

    @mock.patch("tap_amplitude.singer.write_state")
    @mock.patch("tap_amplitude.singer.write_schema")
    @mock.patch("tap_amplitude.do_sync_incremental", return_value=5)
    def test_do_sync_selected_and_unselected_streams(self, mock_do_sync_incremental, mock_write_schema, mock_write_state):
        selected = _entry("events", selected=True)
        unselected = _entry("other", selected=False)
        catalog = Catalog([selected, unselected])

        tap_amplitude.do_sync(mock.MagicMock(), catalog, {})

        mock_do_sync_incremental.assert_called_once()
        self.assertEqual(1, mock_write_schema.call_count)
        self.assertGreaterEqual(mock_write_state.call_count, 2)

    @mock.patch("tap_amplitude.LOGGER.critical")
    @mock.patch("tap_amplitude.do_sync_incremental", side_effect=RuntimeError("boom"))
    @mock.patch("tap_amplitude.singer.write_schema")
    @mock.patch("tap_amplitude.singer.write_state")
    def test_do_sync_raises_on_stream_failure(self, _mock_write_state, _mock_write_schema, _mock_sync_incremental, mock_critical):
        catalog = Catalog([_entry("events", selected=True)])

        with self.assertRaises(RuntimeError):
            tap_amplitude.do_sync(mock.MagicMock(), catalog, {})

        mock_critical.assert_called_once()

    @mock.patch("tap_amplitude.do_discover")
    @mock.patch("tap_amplitude.connect_with_backoff")
    @mock.patch("tap_amplitude.utils.parse_args")
    def test_main_impl_discover_branch(self, mock_parse_args, mock_connect, mock_do_discover):
        args = mock.MagicMock()
        args.config = {"a": "b"}
        args.discover = True
        args.catalog = None
        args.properties = None
        args.state = None
        mock_parse_args.return_value = args
        mock_connect.return_value = mock.MagicMock()

        tap_amplitude.main_impl()

        mock_do_discover.assert_called_once()

    @mock.patch("tap_amplitude.do_sync")
    @mock.patch("tap_amplitude.connect_with_backoff")
    @mock.patch("tap_amplitude.utils.parse_args")
    def test_main_impl_catalog_branch(self, mock_parse_args, mock_connect, mock_do_sync):
        args = mock.MagicMock()
        args.config = {"a": "b"}
        args.discover = False
        args.catalog = Catalog([])
        args.properties = None
        args.state = {"x": 1}
        mock_parse_args.return_value = args
        mock_connect.return_value = mock.MagicMock()

        tap_amplitude.main_impl()

        mock_do_sync.assert_called_once()

    @mock.patch("tap_amplitude.Catalog.from_dict", return_value=Catalog([]))
    @mock.patch("tap_amplitude.do_sync")
    @mock.patch("tap_amplitude.connect_with_backoff")
    @mock.patch("tap_amplitude.utils.parse_args")
    def test_main_impl_properties_branch(self, mock_parse_args, mock_connect, mock_do_sync, _mock_from_dict):
        args = mock.MagicMock()
        args.config = {"a": "b"}
        args.discover = False
        args.catalog = None
        args.properties = {"streams": []}
        args.state = None
        mock_parse_args.return_value = args
        mock_connect.return_value = mock.MagicMock()

        tap_amplitude.main_impl()

        mock_do_sync.assert_called_once()

    @mock.patch("tap_amplitude.LOGGER.info")
    @mock.patch("tap_amplitude.connect_with_backoff")
    @mock.patch("tap_amplitude.utils.parse_args")
    def test_main_impl_no_selection(self, mock_parse_args, mock_connect, mock_info):
        args = mock.MagicMock()
        args.config = {"a": "b"}
        args.discover = False
        args.catalog = None
        args.properties = None
        args.state = None
        mock_parse_args.return_value = args
        mock_connect.return_value = mock.MagicMock()

        tap_amplitude.main_impl()

        mock_info.assert_called_once_with("No properties were selected")

    @mock.patch("tap_amplitude.LOGGER.critical")
    @mock.patch("tap_amplitude.main_impl", side_effect=RuntimeError("fail"))
    def test_main_logs_and_reraises(self, _mock_main_impl, mock_critical):
        with self.assertRaises(RuntimeError):
            tap_amplitude.main()

        mock_critical.assert_called_once()
