import pendulum
import singer

from tap_marketo.client import Client
from tap_marketo.discover import discover
from tap_marketo.sync import sync


REQUIRED_CONFIG_KEYS = [
    "start_date",
    "endpoint",
    "identity",
    "client_id",
    "client_secret",
]

NO_CORONA_CODE = "1035"

LOGGER = singer.get_logger()


def corona_supported(client):
    start_pen = pendulum.utcnow().subtract(days=1).replace(microsecond=0)
    end_pen = start_pen.add(seconds=1)
    payload = {
        "format": "CSV",
        "fields": ["id"],
        "filter": {
            "updatedAt": {
                "startAt": start_pen.isoformat(),
                "endAt": end_pen.isoformat(),
            },
        },
    }
    endpoint = client.get_bulk_endpoint("leads", "create")
    data = client.request("POST", endpoint, json=payload)

    if "errors" in data:
        for err in data["errors"]:
            if err["code"] == NO_CORONA_CODE:
                return False

    endpoint = client.get_bulk_endpoint("leads", "cancel", data["exportId"])
    client.request("POST", endpoint)
    return True


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
        # singer-python's Catalog class doesn't provide much use to this tap, so we
        # treat the catalog simply as a data structure.
        if isinstance(catalog, singer.catalog.Catalog):
            catalog = catalog.to_dict()

        state = validate_state(config, catalog, state)

        # Corona allows us to do bulk queries for Leads using updatedAt as a filter.
        # Clients without Corona (should only be clients with < 50,000 Leads) must
        # do a full Leads bulk export every sync.
        state["use_corona"] = corona_supported(client)

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
