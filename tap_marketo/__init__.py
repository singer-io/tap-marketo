#!/usr/bin/env python3

# Marketo Docs are located at http://developers.marketo.com/rest-api/

import json
import sys
import pendulum
import singer
from singer import bookmarks

from tap_marketo.client import Client
from tap_marketo.discover import discover as _discover
from tap_marketo.sync import sync as _sync
from tap_marketo.sync import determine_replication_key
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


def do_discover(client):
    """
    Call the discovery function.
    """
    catalog = _discover(client)
    # Dump catalog
    json.dump(catalog, sys.stdout, indent=2)


def main():
    """
    Run discover mode or sync mode.
    """
    args = singer.utils.parse_args(REQUIRED_CONFIG_KEYS)

    config = args.config

    client = Client(config)

    state = {}
    if args.state:
        state = args.state

    if args.discover:
        do_discover(client)
    else:
        catalog = args.properties if args.properties else _discover(client)
        _sync(client, config, state, catalog)


if __name__ == "__main__":
    main()
