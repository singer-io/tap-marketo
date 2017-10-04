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
        if not stream.get("selected"):
            # If a stream is deselected while it's the current stream,
            # unset the current stream.
            if stream["tap_stream_id"] == state["current_stream"]:
                state["current_stream"] = None

            continue

        if stream["tap_stream_id"] not in state["bookmarks"]:
            state["bookmarks"][stream["tap_stream_id"]] = {
                stream["replication_key"]: config["start_date"],
            }

    return state


def main(config, catalog, state, discover_mode=False):
    client = Client.from_config(config)

    if discover_mode:
        discover(client)
    elif catalog:
        if isinstance(catalog, singer.catalog.Catalog):
            catalog = catalog.to_dict()
        state = validate_state(config, catalog, state)
        sync(client, catalog, state)
    else:
        raise Exception("Must have catalog if syncing")


if __name__ == "__main__":
    args = singer.utils.parse_args(REQUIRED_CONFIG_KEYS)

    try:
        main(args.config, args.catalog, args.state, args.discover)
    except Exception as e:
        LOGGER.fatal(e)
        raise e
