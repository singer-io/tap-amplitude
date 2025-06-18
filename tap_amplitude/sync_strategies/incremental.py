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
    # FIX: Safely parse schema and table from tap_stream_id
    tap_stream_id = catalog_entry.tap_stream_id

    if '-' in tap_stream_id:
        schema, table = tap_stream_id.split('-', 1)
        full_table_name = f'"{schema}"."{table}"'
    else:
        full_table_name = f'"{tap_stream_id}"'

    select_sql = f"""
                SELECT {','.join(columns)}
                    FROM {full_table_name}
                """
    LOGGER.debug(f"Constructed SELECT SQL: {select_sql}")
    return select_sql


def process_row(row, columns):
    utc = pytz.timezone('UTC')
    row_as_list = list(row)

    for i in range(len(row_as_list)):
        # FIX: Handle both datetime and date
        if isinstance(row_as_list[i], datetime.datetime):
            row_as_list[i] = utils.strftime(utc.localize(row_as_list[i]))
        elif isinstance(row_as_list[i], datetime.date):
            row_as_list[i] = row_as_list[i].isoformat()

    return dict(zip(columns, row_as_list))


def sync_table(connection, catalog_entry, state, columns):
    # If there is an existing bookmark, use it; otherwise, use replication_key
    replication_key_value = None
    if not state.get('bookmarks', {}).get(catalog_entry.tap_stream_id):
        singer.write_bookmark(state,
                              catalog_entry.tap_stream_id,
                              catalog_entry.replication_key,
                              None)
    else:
        # Start with the bookmark.
        replication_key_value = singer.get_bookmark(state,
                                                    catalog_entry.tap_stream_id,
                                                    catalog_entry.replication_key)

    # with connection.cursor() as cursor:
    cursor = connection.cursor()

    # Build the sql for this stream.
    select_sql = generate_select_sql(catalog_entry, columns)

    # If bookmark exists, modify the query.
    if replication_key_value is not None:
        if catalog_entry.schema.properties[catalog_entry.replication_key].format == 'date-time':
            replication_key_value = pendulum.parse(replication_key_value)

        select_sql += " WHERE {} >= '{}' ORDER BY {} ASC".format(
                              catalog_entry.replication_key,
                              replication_key_value,
                              catalog_entry.replication_key)

    elif catalog_entry.replication_key is not None:
        select_sql += ' ORDER BY {} ASC'.format(catalog_entry.replication_key)

    # time to sync.
    LOGGER.info('Running %s', select_sql)
    cursor.execute(select_sql)

    row = cursor.fetchone()
    rows_saved = 0

    with metrics.record_counter(catalog_entry.tap_stream_id) as counter:
        counter.tags['table'] = catalog_entry.table
        while row:
            counter.increment()
            rows_saved += 1

            # format record
            rec = process_row(row, columns)

            # resolve against metadata
            with Transformer() as transformer:
                rec = transformer.transform(rec, catalog_entry.schema.to_dict(), metadata.to_map(catalog_entry.metadata))

            # write to singer.
            singer.write_record(catalog_entry.stream, rec)

            # Perhaps the more modern way of managing state.
            if catalog_entry.replication_method == "INCREMENTAL":
                if catalog_entry.replication_key in rec:
                    singer.write_bookmark(state,
                                          catalog_entry.tap_stream_id,
                                          catalog_entry.replication_key,
                                          rec[catalog_entry.replication_key])
                else:
                    LOGGER.warning("Replication key %s missing in record for stream %s",
                                   catalog_entry.replication_key, catalog_entry.stream)

            if rows_saved % 100 == 0:
                singer.write_state(state)

            row = cursor.fetchone()

        singer.write_state(state)

    return counter.value
