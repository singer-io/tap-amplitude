#!/usr/bin/env python3
# pylint: disable=missing-docstring,not-an-iterable,too-many-locals,too-many-arguments,too-many-branches,invalid-name,duplicate-code,too-many-statements

from pprint import pprint
import snowflake.connector

import datetime
import collections
import itertools
from itertools import dropwhile
import copy

import pendulum

import singer
import singer.metrics as metrics
import singer.schema

from singer import utils
from singer import metadata
from singer import bookmarks
from singer import Transformer
from singer.schema import Schema
from singer.catalog import Catalog, CatalogEntry

from tap_amplitude.connection import connect_with_backoff
import tap_amplitude.sync_strategies.incremental as sync_incremental

Column = collections.namedtuple('Column', [
    "table_schema",
    "table_name",
    "column_name",
    "data_type",
    "character_maximum_length",
    "numeric_precision",
    "numeric_scale",
    "column_type",
    "column_key"])

REQUIRED_CONFIG_KEYS = [
    'account',
    'warehouse',
    'database',
    'username',
    'password'
]

LOGGER = singer.get_logger()


Column = collections.namedtuple('Column', [
    "table_schema",
    "table_name",
    "column_name",
    "data_type",
    "character_maximum_length",
    "numeric_precision",
    "numeric_scale"])


def schema_for_column(c, inclusion='available'):
    '''Returns the Schema object for the given Column.'''
    data_type = c.data_type.lower()

    result = Schema(inclusion=inclusion)

    if data_type == 'boolean':
        result.type = ['null', 'boolean']

    elif data_type == 'number' or data_type == 'real' or data_type == 'float' or data_type == 'fixed':
        result.type = ['null', 'number']

    elif data_type == 'text':
        result.type = ['null', 'string']

    elif data_type == 'timestamp_ntz':
        result.type = ['null', 'string']
        result.format = 'date-time'

    elif data_type == 'variant' or data_type == 'array':
        result.type = ['null', 'string']

    else:
        result = Schema(None,
                        inclusion='unsupported',
                        description='Unsupported column type {}'.format(data_type))

    return result


def create_column_metadata(cols):
    mdata = {}
    for c in cols:
        schema = schema_for_column(c)
        mdata = metadata.write(mdata,
                               ('properties', c.column_name),
                               'inclusion',
                               schema.inclusion)

        # TODO:
        #   {"invalid_fields":"Non-discoverable metadata can not be discovered:
        # mdata = metadata.write(mdata,
        #                        ('properties', c.column_name),
        #                        'datatype',
        #                        c.data_type.lower())

    return metadata.to_list(mdata)


def discover_catalog(connection):
    cursor = connection.cursor()
    cursor.execute("""
        SELECT table_schema,
               table_name,
               column_name,
               data_type,
               character_maximum_length,
               numeric_precision,
               numeric_scale
            FROM information_schema.columns
            WHERE table_schema != 'INFORMATION_SCHEMA'
            ORDER BY table_schema, table_name
        """)

    columns = []
    rec = cursor.fetchone()
    while rec is not None:
        columns.append(Column(*rec))
        rec = cursor.fetchone()

    entries = []
    for (k, cols) in itertools.groupby(columns, lambda c: (c.table_schema, c.table_name)):
        cols = list(cols)
        (table_schema, table_name) = k
        md = create_column_metadata(cols)
        md_map = metadata.to_map(md)

        if "events" in table_name.lower():
            key_properties = ['UUID']
            valid_replication_keys = ["SERVER_UPLOAD_TIME"]
            replication_key = "SERVER_UPLOAD_TIME"
        elif "merge" in table_name.lower():
            key_properties = []
            valid_replication_keys = ["MERGE_EVENT_TIME"]
            replication_key = "MERGE_EVENT_TIME"
        else:
            replication_key = ""
            key_properties = []
            valid_replication_keys = []

        properties = {}
        for c in cols:
            if c.column_name == replication_key or c.column_name in key_properties:
                properties[c.column_name] = schema_for_column(c, "automatic")
            else:
                properties[c.column_name] = schema_for_column(c, "available")
        schema = Schema(type='object', properties=properties)

        md_map = metadata.write(md_map,
                                (),
                                'table-key-properties',
                                key_properties)

        md_map = metadata.write(md_map,
                                (),
                                'valid-replication-keys',
                                valid_replication_keys)

        md_map = metadata.write(md_map,
                                ('properties', replication_key),
                                'inclusion',
                                'automatic')

        entry = CatalogEntry(
            stream=table_name,
            metadata=metadata.to_list(md_map),
            tap_stream_id=table_schema + "-" + table_name,
            replication_key=replication_key, # This is a non-discoverable key.
            replication_method="INCREMENTAL", # This is a non-discoverable key.
            schema=schema)

        entries.append(entry)

    return Catalog(entries)


def do_discover(connection):
    discover_catalog(connection).dump()


def get_key_properties(catalog_entry):
    catalog_metadata = metadata.to_map(catalog_entry.metadata)
    stream_metadata = catalog_metadata.get((), {})
    key_properties = stream_metadata.get('table-key-properties', [])
    return key_properties


def do_sync_incremental(con, catalog_entry, state, columns):
    LOGGER.info("Stream %s is using incremental replication", catalog_entry.stream)
    key_properties = get_key_properties(catalog_entry)

    if not catalog_entry.replication_key:
        raise Exception("Cannot use INCREMENTAL replication for table ({}) without a replication key.".format(catalog_entry.stream))

    singer.write_schema(catalog_entry.stream,
                        catalog_entry.schema.to_dict(),
                        key_properties,
                        [catalog_entry.replication_key])

    with metrics.job_timer('sync_table') as timer:
        timer.tags['table'] = catalog_entry.table
        return sync_incremental.sync_table(con, catalog_entry, state, columns)


def stream_is_selected(mdata):
    return mdata.get((), {}).get('selected', False)


def do_sync(con, catalog, state):
    for catalog_entry in catalog.streams:
        stream_name = catalog_entry.tap_stream_id
        mdata = metadata.to_map(catalog_entry.metadata)
        if not stream_is_selected(mdata):
            LOGGER.info("%s: Skipping - not selected", stream_name)
            continue

        singer.write_state(state)
        key_properties = metadata.get(mdata, (), 'table-key-properties')
        singer.write_schema(stream_name, catalog_entry.schema.to_dict(), key_properties)
        columns = list(catalog_entry.schema.properties.keys())

        LOGGER.info("%s: Starting sync", stream_name)
        counter_value = do_sync_incremental(con, catalog_entry, state, columns)
        if counter_value is not None:
            LOGGER.info("%s: Completed sync (%s rows)" , stream_name, counter_value)
        else:
            LOGGER.info("%s: Completed sync (%s rows)", stream_name, counter_value)

    singer.write_state(state)
    LOGGER.info("Finished sync")


def main_impl():
    args = utils.parse_args(REQUIRED_CONFIG_KEYS)
    con = connect_with_backoff(args.config)

    if args.discover:
        do_discover(con)
    elif args.catalog:
        state = args.state or {}
        do_sync(con, args.catalog, state)
    elif args.properties:
        catalog = Catalog.from_dict(args.properties)
        state = args.state or {}
        do_sync(con, catalog, state)
    else:
        LOGGER.info("No properties were selected")


def main():
    try:
        main_impl()
    except Exception as exc:
        LOGGER.critical(exc)
        raise exc
