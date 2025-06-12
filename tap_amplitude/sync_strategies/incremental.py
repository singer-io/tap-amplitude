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


def generate_select_sql(catalog_entry, columns):
    table = catalog_entry.stream
    catalog_metadata = metadata.to_map(catalog_entry.metadata)
    stream_metadata = catalog_metadata.get((), {})
    schema = stream_metadata.get('schema-name')

    select_sql = """
                SELECT {}
                    FROM {}
                """.format(','.join(columns), catalog_entry.tap_stream_id.replace('-', '.'))

    return select_sql


def process_row(row, columns):
    return dict(zip(columns, list(row)))


def sync_table(connection, catalog_entry, state, columns):
    replication_key_value = None
    if not state.get('bookmarks', {}).get(catalog_entry.tap_stream_id):
        singer.write_bookmark(state,
                              catalog_entry.tap_stream_id,
                              catalog_entry.replication_key,
                              None)
    else:
        replication_key_value = singer.get_bookmark(state,
                                                    catalog_entry.tap_stream_id,
                                                    catalog_entry.replication_key)

    cursor = connection.cursor()
    select_sql = generate_select_sql(catalog_entry, columns)

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
    cursor.execute(select_sql)

    row = cursor.fetchone()
    rows_saved = 0

    with metrics.record_counter(catalog_entry.tap_stream_id) as counter:
        counter.tags['table'] = catalog_entry.table
        while row:
            counter.increment()
            rows_saved += 1

            rec = process_row(row, columns)

            # Convert datetime.date and datetime.datetime to ISO strings
            for k, v in rec.items():
                if isinstance(v, (datetime.datetime, datetime.date)):
                    rec[k] = v.isoformat()

            singer.write_record(catalog_entry.stream, rec)  # ✅ This emits the record to output

            if catalog_entry.replication_method == "INCREMENTAL":
                singer.write_bookmark(state,
                                      catalog_entry.tap_stream_id,
                                      catalog_entry.replication_key,
                                      rec[catalog_entry.replication_key])

            if rows_saved % 100 == 0:
                singer.write_state(state)

            row = cursor.fetchone()

        singer.write_state(state)

        return rows_saved
