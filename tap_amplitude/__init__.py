#!/usr/bin/env python3

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

from singer import utils, metadata, bookmarks
from singer import Transformer
from singer.schema import Schema
from singer.catalog import Catalog, CatalogEntry

from tap_amplitude.connection import connect_with_backoff
import tap_amplitude.sync_strategies.incremental as sync_incremental

# Updated: Removed unused fields from Column to match SELECT clause
Column = collections.namedtuple('Column', [
    "table_schema",
    "table_name",
    "column_name",
    "data_type",
    "character_maximum_length",
    "numeric_precision",
    "numeric_scale"
])

REQUIRED_CONFIG_KEYS = ['account', 'warehouse', 'database', 'username', 'password']
LOGGER = singer.get_logger()


def schema_for_column(c, inclusion='available'):
    STRING_SCHEMA = {'type': ['null', 'string']}
    NUMBER_SCHEMA = {'type': ['null', 'number']}
    DATETIME_SCHEMA = {'type': ['null', 'string'], 'format': 'date-time'}

    SCHEMA_MAPPING = {
        'boolean': {'type': ['null', 'boolean']},
        'number': NUMBER_SCHEMA,
        'real': NUMBER_SCHEMA,
        'float': NUMBER_SCHEMA,
        'fixed': NUMBER_SCHEMA,
        'integer': NUMBER_SCHEMA,
        'numeric': NUMBER_SCHEMA,
        'text': STRING_SCHEMA,
        'string': STRING_SCHEMA,
        'timestamp_ntz': DATETIME_SCHEMA,
        'datetime': DATETIME_SCHEMA,
        'date': DATETIME_SCHEMA,
        'variant': STRING_SCHEMA,
        'array': STRING_SCHEMA,
        'object': STRING_SCHEMA,
    }

    data_type = c.data_type.lower()
    schema_kwargs = SCHEMA_MAPPING.get(data_type)

    if schema_kwargs:
        return Schema(**schema_kwargs, inclusion=inclusion)

    return Schema(None, inclusion='unsupported', description=f'Unsupported column type {data_type}')


def create_column_metadata(cols):
    mdata = {}
    for c in cols:
        schema = schema_for_column(c)
        mdata = metadata.write(mdata, ('properties', c.column_name), 'inclusion', schema.inclusion)
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

    # Replaces: while-loop based column appending in old version
    columns = [Column(*rec) for rec in cursor]
    entries = []

    for (schema, table), cols_iter in itertools.groupby(columns, key=lambda c: (c.table_schema, c.table_name)):
        cols = list(cols_iter)
        available_cols = {c.column_name.upper(): c for c in cols}

        # NEW: Dynamically detect replication key instead of hardcoding table name logic
        preferred_keys = ["SERVER_UPLOAD_TIME", "TIME_CREATED", "EVENT_TIME", "MERGE_EVENT_TIME"]
        rk = next((k for k in preferred_keys if k in available_cols), "")
        if not rk:
            for col in available_cols:
                if col.endswith(("_TIME", "_TS", "_DATE")):
                    rk = col
                    break  # NEW: Fallback logic for timestamp-based key detection

        replication_key = rk  # Similar role as old 'replication_key = ...'
        replication_method = "INCREMENTAL" if replication_key else "FULL_TABLE"
        key_properties = ["UUID"] if "UUID" in available_cols else []  # Replaces old key_properties logic

        properties = {}
        for c in cols:
            incl = "automatic" if c.column_name.upper() in {replication_key, *key_properties} else "available"
            properties[c.column_name] = schema_for_column(c, incl)

        schema_obj = Schema(type="object", properties=properties)

        # Metadata creation and enhancement
        md_map = metadata.to_map(create_column_metadata(cols))
        md_map = metadata.write(md_map, (), "table-key-properties", key_properties)
        md_map = metadata.write(md_map, (), "valid-replication-keys", [replication_key] if replication_key else [])
        if replication_key:
            md_map = metadata.write(md_map, ("properties", replication_key), "inclusion", "automatic")

        # Final catalog entry
        entry = CatalogEntry(
            stream=table,
            tap_stream_id=f"{schema}-{table}",
            schema=schema_obj,
            metadata=metadata.to_list(md_map),
            replication_key=replication_key,
            replication_method=replication_method
        )

        # NEW: Select stream and fields by default (replaces CLI args or manual selection)
        entry.selected = True
        for md in entry.metadata:
            if md["metadata"].get("inclusion") != "unsupported":
                md["metadata"]["selected"] = True

        entries.append(entry)

    return Catalog(entries)



def do_discover(connection):
    discover_catalog(connection).dump()


def get_key_properties(catalog_entry):
    catalog_metadata = metadata.to_map(catalog_entry.metadata)
    return catalog_metadata.get((), {}).get('table-key-properties', [])


def do_sync_incremental(con, catalog_entry, state, columns):
    LOGGER.info("Stream %s is using incremental replication", catalog_entry.stream)
    key_properties = get_key_properties(catalog_entry)

    if not catalog_entry.replication_key:
        raise Exception(f"Cannot use INCREMENTAL replication for table ({catalog_entry.stream}) without a replication key.")

    singer.write_schema(
        catalog_entry.stream,
        catalog_entry.schema.to_dict(),
        key_properties,
        [catalog_entry.replication_key]
    )

    with metrics.job_timer('sync_table') as timer:
        timer.tags['table'] = catalog_entry.stream
        records = sync_incremental.sync_table(con, catalog_entry, state, columns)

        if records is None:
            LOGGER.warning("sync_table returned None. Treating as empty.")
            return []

        return records


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

        try:
            rows_synced = do_sync_incremental(con, catalog_entry, state, columns)
            LOGGER.info("%s: Completed sync (%s rows)", stream_name, rows_synced)
        except Exception as e:
            LOGGER.critical("Error syncing stream %s: %s", stream_name, str(e))
            raise

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
