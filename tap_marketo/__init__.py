import pendulum
import singer

from singer import bookmarks
from tap_marketo.client import Client
from tap_marketo.discover import discover
from tap_marketo.sync import sync


REQUIRED_CONFIG_KEYS = [
    "start_date",

    # Log in to Marketo
    # Go to Admin, select Integration->Web Services
    # Endpoint url matches https://123-ABC-456.mktorest.com/rest
    # Domain is the 9 character alpha numeric part of the url
    "domain",

    # Log in to Marketo
    # Go to Admin, select Integration->Launch Point
    # Client ID and Secret can be found by clicking "View Details"
    "client_id",
    "client_secret",
]

LOGGER = singer.get_logger()


def validate_state(config, catalog, state):
    for stream in catalog["streams"]:
        if not stream["schema"].get("selected"):
            # If a stream is deselected while it's the current stream, unset the
            # current stream.
            if stream["tap_stream_id"] == bookmarks.get_currently_syncing(state):
                bookmarks.set_currently_syncing(state, None)
            continue

        # If there's no bookmark for a stream (new integration, newly selected,
        # reset, etc) we need to use the default start date from the config.
        if bookmarks.get_bookmark(state, stream["tap_stream_id"], \
                                  stream.get("replication_key")) is None:
            
            state = bookmarks.write_bookmark(state, stream["tap_stream_id"], \
                                     stream.get("replication_key"), config["start_date"])

    singer.write_state(state)
    return state


def _main(config, catalog, state, discover_mode=False):
    client = Client(**config)
    if discover_mode:
        discover(client)
    elif catalog:
        if isinstance(catalog, singer.catalog.Catalog):
            catalog = catalog.to_dict()
        state = validate_state(config, catalog, state)
        sync(client, catalog, state)
    else:
        raise Exception("Must have catalog if syncing")


def main():
    args = singer.utils.parse_args(REQUIRED_CONFIG_KEYS)

    try:
        _main(args.config, args.properties, args.state, args.discover)
    except Exception as e:
        LOGGER.critical(e)
        raise e


if __name__ == "__main__":
    main()
