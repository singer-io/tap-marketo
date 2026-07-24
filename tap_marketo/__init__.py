#!/usr/bin/env python3

# Marketo Docs are located at http://developers.marketo.com/rest-api/

import json
import sys

import pendulum
import singer
from singer import bookmarks

from tap_marketo.client import Client
from tap_marketo.discover import discover
from tap_marketo.sync import sync, determine_replication_key
from singer.bookmarks import (
    get_bookmark,
    write_bookmark,
    get_currently_syncing,
    set_currently_syncing,
)

REQUIRED_CONFIG_KEYS = [
    "start_date",

    # Log in to Marketo
    # Go to Admin, select Integration->Web Services
    # Endpoint url matches https://123-ABC-456.mktorest.com/rest
    "endpoint",

    # Log in to Marketo
    # Go to Admin, select Integration->Launch Point
    # Client ID and Secret can be found by clicking "View Details"
    "client_id",
    "client_secret",
]


def validate_state(config, catalog, state):
    streams = catalog.get("streams", []) if isinstance(catalog, dict) else getattr(catalog, "streams", [])
    for stream in streams:
        stream_id = stream.get("tap_stream_id") if isinstance(stream, dict) else getattr(stream, "tap_stream_id", None)
        stream_metadata = stream.get("metadata", []) if isinstance(stream, dict) else getattr(stream, "metadata", [])

        for mdata in stream_metadata:
            breadcrumb = mdata.get('breadcrumb') if isinstance(mdata, dict) else getattr(mdata, 'breadcrumb', None)
            mdata_metadata = mdata.get('metadata', {}) if isinstance(mdata, dict) else getattr(mdata, 'metadata', {})

            if breadcrumb == [] and mdata_metadata.get('selected') != True:
                # If a stream is deselected while it's the current stream, unset the
                # current stream.
                if stream_id == get_currently_syncing(state):
                    set_currently_syncing(state, None)
                break

        replication_key = determine_replication_key(stream_id)
        if not replication_key:
            continue

        # If there's no bookmark for a stream (new integration, newly selected,
        # reset, etc) we need to use the default start date from the config.
        bookmark = get_bookmark(state,
                                stream_id,
                                replication_key)
        if bookmark is None:
            state = write_bookmark(state,
                                   stream_id,
                                   replication_key,
                                   config["start_date"])

    singer.write_state(state)
    return state

def _main(config, properties, state, discover_mode=False):
    client = Client(**config)
    if discover_mode:
        catalog = discover(client)
        json.dump(catalog, sys.stdout, indent=2)
    elif properties:
        state = validate_state(config, properties, state)
        sync(client, properties, config, state)


def main():
    args = singer.utils.parse_args(REQUIRED_CONFIG_KEYS)

    try:
        _main(args.config, args.properties or args.catalog, args.state, args.discover)
    except Exception as e:
        singer.log_critical(e)
        raise e


if __name__ == "__main__":
    main()
