import pendulum
import singer

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
    if "current_stream" not in state:
        state["current_stream"] = None

    if "bookmarks" not in state:
        state["bookmarks"] = {}

    for stream in catalog["streams"]:
        if not stream["schema"].get("selected"):
            # If a stream is deselected while it's the current stream, unset the
            # current stream.
            if stream["stream"] == state["current_stream"]:
                state["current_stream"] = None
            continue

        # If there's no bookmark for a stream (new integration, newly selected,
        # reset, etc) we need to use the default start date from the config.
        if stream["stream"] not in state["bookmarks"] and stream.get("replication_key"):
            state["bookmarks"][stream["stream"]] = {
                stream["replication_key"]: config["start_date"],
            }

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
        _main(args.config, args.catalog, args.state, args.discover)
    except Exception as e:
        LOGGER.fatal(e)
        raise e


if __name__ == "__main__":
    main()
