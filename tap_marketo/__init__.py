import singer
from singer import metrics
from singer.catalog import Catalog

from tap_marketo.client import Client
from tap_marketo.discover import discover_entities
from tap_marketo.entity import Entity, LeadEntity, ActivityEntity
from tap_marketo.state import State
from tap_marketo.streamer import BulkStreamer, RestStreamer


REQUIRED_CONFIG_KEYS = [
    "endpoint",
    "identity".
    "client_id",
    "client_secret",
    "start_date",
]

DEFAULT_MAX_DAILY_CALLS = 8000

ENTITY_CLASS_OVERRIDES = {
    "leads": LeadEntity,
    "activities": ActivityEntity,
}

STREAMER_CLASS_OVERRIDES = {
    "leads": BulkStreamer,
    "activities": BulkStreamer,
}



def do_discover(client):
    LOGGER.info("Starting discover")
    catalog = discover_entities(client)
    catalog.dump()
    LOGGER.info("Finished discover")


def do_sync(client, state, catalog, ignore_lead_activities=False):
    LOGGER.info("Starting sync")

    started = False
    for catalog_entry in catalog.streams:
        if not started and state.current_stream and catalog_entry.tap_stream_id != state.current_stream:
            continue
        else:
            started = True

        if not catalog_entry.selected:
            LOGGER.info("%s not selected. Skipping.", catalog_entry.tap_stream_id)
            continue
        else:
            LOGGER.info("Syncing %s", catalog_entry.tap_stream_id)

        EntityClass = ENTITY_CLASS_OVERRIDES.get(catalog_entry.tap_stream_id, Entity)
        entity = EntityClass.from_catalog_entry(catalog_entry)
        entity.ignore_lead_activities = ignore_lead_activities

        StreamerClass = STREAMER_CLASS_OVERRIDES.get(catalog_entry.tap_stream_id, RestStreamer)
        streamer = StreamerClass(entity, client, state)

        state.current_stream = entity.name
        singer.write_state(state.to_dict())

        singer.write_schema(entity.name, entity.schema.to_dict(), entity.key_properties)
        with metrics.record_counter(entity.name) as counter:
            for record in streamer:
                singer.write_record(entity.name, record)
                entity.update_bookmark(record, state)
                singer.write_state(state.to_dict())
                counter.increment()

        LOGGER.info("Finished syncing %s %s", counter.value, entity.name)
        state.current_stream = None
        singer.write_state(state.to_dict())

    LOGGER.info("Finished sync")
    singer.write_state(state.to_dict())


def main(config, state=None, properties=None, discover=False):
    client = Client(
        endpoint=config["endpoint"],
        identity=config["identity"],
        client_id=config["client_id"],
        client_secret=config["client_secret"],
        max_daily_calls=config.get("max_daily_calls", DEFAULT_MAX_DAILY_CALLS),
        user_agent=config.get("user_agent", "Singer.io/tap-marketo"),
    )

    state = state or {}
    ignore_lead_activities = config.get("ignore_lead_activities", False)

    if discover:
        do_discover(client)

    elif properties:
        state = State.from_dict(state)
        state.default_start_date = config["start_date"]

        catalog = Catalog.from_dict(properties)
        do_sync(client, state, catalog, ignore_lead_activities)

    else:
        raise Exception("Must have properties or run discovery")


if __name__ == "__main__":
    args = singer.utils.parse_args(REQUIRED_CONFIG_KEYS)
    main(args.config, args.state, args.properties, args.discover)
