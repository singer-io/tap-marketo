#!/usr/bin/env python3

# Marketo Docs are located at http://developers.marketo.com/rest-api/

import pendulum
import singer
from singer import bookmarks
from singer import logger

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
    singer.log_info("state in validate state: %s" % state)
    for stream in catalog["streams"]:
        if stream['tap_stream_id'] == 'deleted_leads':
            singer.log_info("state in validate_state in for loop: %s" % state)
        for mdata in stream['metadata']:
            if mdata['breadcrumb'] == [] and mdata['metadata'].get('selected') != True:
                # If a stream is deselected while it's the current stream, unset the
                # current stream.
                singer.log_info("In mdata['breadcrumb'] == [] and mdata['metadata'].get('selected') != True:")
                if stream["tap_stream_id"] == get_currently_syncing(state):
                    singer.log_info("stream[tap_stream_id] == get_currently_syncing: %s" % stream["tap_stream_id"])
                    set_currently_syncing(state, None)
                break

        replication_key = determine_replication_key(stream['tap_stream_id'])
        if not replication_key:
            continue

        # If there's no bookmark for a stream (new integration, newly selected,
        # reset, etc) we need to use the default start date from the config.
        bookmark = get_bookmark(state,
                                stream["tap_stream_id"],
                                replication_key)
        singer.log_info("bookmark: %s" % bookmark)
        if bookmark is None:
            singer.log_info("No bookmark found for stream: %s, setting to start_date" % stream["tap_stream_id"])
            # singer.log_info("state: %s" % state)
            state = write_bookmark(state,
                                   stream["tap_stream_id"],
                                   replication_key,
                                   config["start_date"])

    singer.write_state(state)
    return state

def _main(config, properties, state, discover_mode=False):
    client = Client(**config)
    if discover_mode:
        discover(client)
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
