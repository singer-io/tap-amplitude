import unittest

from singer.catalog import CatalogEntry, Schema

import tap_amplitude.sync_strategies.incremental as incremental


def _entry_with_metadata():
    schema = Schema.from_dict(
        {
            "type": "object",
            "properties": {
                "UUID": {"type": ["null", "string"]},
                "SERVER_UPLOAD_TIME": {"type": ["null", "string"], "format": "date-time"},
                "EXTRA": {"type": ["null", "string"]},
            },
        }
    )
    return CatalogEntry(
        stream="events",
        tap_stream_id="PUBLIC-events",
        schema=schema,
        metadata=[
            {"breadcrumb": [], "metadata": {"selected": True}},
            {"breadcrumb": ["properties", "UUID"], "metadata": {"inclusion": "automatic"}},
            {"breadcrumb": ["properties", "SERVER_UPLOAD_TIME"], "metadata": {"inclusion": "automatic"}},
            {"breadcrumb": ["properties", "EXTRA"], "metadata": {"selected": False}},
        ],
        replication_key="SERVER_UPLOAD_TIME",
        replication_method="INCREMENTAL",
    )


class TestAutomaticFields(unittest.TestCase):
    def test_automatic(self):
        entry = _entry_with_metadata()
        selected_columns = incremental.get_selected_columns(
            entry, ["UUID", "SERVER_UPLOAD_TIME", "EXTRA"]
        )

        self.assertEqual(["UUID", "SERVER_UPLOAD_TIME"], selected_columns)
