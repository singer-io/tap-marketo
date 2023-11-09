#!/usr/bin/env python3

# Marketo Docs are located at http://developers.marketo.com/rest-api/
import itertools
import re

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


ATTRIBUTION_WINDOW_README = """
`attribution_window` may be specified by a combination of days, hours and minutes. This parameter is 
quite useful in a moderate frequency incremental bulk extracts (e.g. once an hour)
to allow users a way to avoid extracting all leads updated 1 day prior (i.e. default attribution window)
examples of valid attribution_windows: 1d, 12h, 1h30m, 1d6h55m
"""
ATTRIBUTION_WINDOW_SPEC_RE_ATOMS = [r'\d{1,2}d', r'\d{1,2}h', r'\d{1,2}m']
ATTRIBUTION_WINDOW_ALLOWED_RE = [
    re.compile(''.join(x))
    for i in range(1, 4)
    for x in itertools.combinations(ATTRIBUTION_WINDOW_SPEC_RE_ATOMS, i)
    ]


def parse_attribution_window(attribution_window_string):
    f"""
    Parse optional config parameter `attribution_window`.

    Attribution window is used to set an earlier export_start
    for incremental replication of of the leads stream.
    
    {ATTRIBUTION_WINDOW_README}
    """
    aw = attribution_window_string.lower().strip()
    if not any(p.match(aw) for p in ATTRIBUTION_WINDOW_ALLOWED_RE):
        raise ValueError(
            f"Invalid attribution window string: {attribution_window_string}."
            f"{ATTRIBUTION_WINDOW_README}"
            )
    window_code_name_map = {'d': 'days', 'h': 'hours', 'm': 'minutes'}
    return {
        window_code_name_map[y]: int(x)
        for x, y in re.findall(r'(\d{1,2})([dhm])', aw)
        }


def validate_state(config, catalog, state):
    for stream in catalog["streams"]:
        for mdata in stream['metadata']:
            if mdata['breadcrumb'] == [] and mdata['metadata'].get('selected') != True:
                # If a stream is deselected while it's the current stream, unset the
                # current stream.
                if stream["tap_stream_id"] == get_currently_syncing(state):
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
        if bookmark is None:
            state = write_bookmark(state,
                                   stream["tap_stream_id"],
                                   replication_key,
                                   config["start_date"])

    singer.write_state(state)
    return state

def _main(config, properties, state, discover_mode=False):
    if 'attribution_window' in config:
        config['attribution_window'] = parse_attribution_window(config['attribution_window'])

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
