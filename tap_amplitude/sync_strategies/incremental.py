#!/usr/bin/env python3
# pylint: disable=duplicate-code

import pytz
import datetime
import pendulum
import singer
import singer.metrics as metrics

from singer import Transformer
from singer import metadata
from singer import utils

from tap_amplitude.connection import connect_with_backoff

LOGGER = singer.get_logger()


def get_selected_columns(catalog_entry, columns):
    metadata_map = metadata.to_map(catalog_entry.metadata)
    selected_columns = []
    for column in columns:
        breadcrumb = ("properties", column)
        column_meta = metadata_map.get(breadcrumb, {})
        inclusion = column_meta.get("inclusion")
        selected = column_meta.get("selected", False)

        if inclusion == "automatic" or selected is True:
            selected_columns.append(column)

    return selected_columns


def generate_select_sql(tap_stream_id, selected_columns):
    if not selected_columns:
        raise ValueError(f"No selected fields found for stream {tap_stream_id}")

    return f"""
                SELECT {','.join(selected_columns)}
                    FROM {tap_stream_id}
           """


def process_row(row, columns):
    return dict(zip(columns, list(row)))


def sync_table(connection, catalog_entry, state, columns):
    replication_key_value = None

    # Bookmark logic
    if not state.get('bookmarks', {}).get(catalog_entry.tap_stream_id):
        singer.write_bookmark(state,
                              catalog_entry.tap_stream_id,
                              catalog_entry.replication_key,
                              None)
    else:
        replication_key_value = singer.get_bookmark(state,
                                                    catalog_entry.tap_stream_id,
                                                    catalog_entry.replication_key)

    # Prepare selected fields for SQL
    selected_columns = get_selected_columns(catalog_entry, columns)
    tap_stream_id = catalog_entry.tap_stream_id.replace('-', '.')
    select_sql = generate_select_sql(tap_stream_id, selected_columns)

    # Apply replication key filtering
    if replication_key_value is not None:
        if catalog_entry.schema.properties[catalog_entry.replication_key].format == 'date-time':
            replication_key_value = pendulum.parse(replication_key_value)

        select_sql += " WHERE {} >= '{}' ORDER BY {} ASC".format(
            catalog_entry.replication_key,
            replication_key_value,
            catalog_entry.replication_key)
    elif catalog_entry.replication_key is not None:
        select_sql += ' ORDER BY {} ASC'.format(catalog_entry.replication_key)

    LOGGER.info('Running %s', select_sql)

    cursor = connection.cursor()
    cursor.execute(select_sql)

    row = cursor.fetchone()
    rows_saved = 0

    with metrics.record_counter(catalog_entry.tap_stream_id) as counter:
        counter.tags['table'] = catalog_entry.stream

        while row:
            counter.increment()
            rows_saved += 1

            rec = process_row(row, selected_columns)

            # Convert datetime/date to ISO strings
            for k, v in rec.items():
                if isinstance(v, (datetime.datetime, datetime.date)):
                    rec[k] = v.isoformat()

            # Apply transformations
            with Transformer() as transformer:
                rec = transformer.transform(rec, catalog_entry.schema.to_dict(), metadata.to_map(catalog_entry.metadata))

            # Write to Singer
            singer.write_record(catalog_entry.tap_stream_id, rec)

            if catalog_entry.replication_method == "INCREMENTAL":
                singer.write_bookmark(state,
                                      catalog_entry.tap_stream_id,
                                      catalog_entry.replication_key,
                                      rec[catalog_entry.replication_key])

            if rows_saved % 100 == 0:
                singer.write_state(state)

            row = cursor.fetchone()

        singer.write_state(state)

    return counter.value
