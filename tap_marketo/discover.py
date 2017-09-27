import functools
import os

from singer.catalog import (
    Catalog,
    CatalogEntry,
)
from singer.schema import Schema


DISCOVER = "discover"
LOAD = "load"

ENTITIES_ACTIONS = [
    ("activity_types", LOAD),
    ("leads", DISCOVER),
    ("activities", LOAD),
    ("lists", LOAD),
    ("campaigns", DISCOVER),
    ("programs", LOAD),
]

STRING_TYPES = [
    "string",
    "email",
    "reference",
    "url",
    "phone",
    "textarea",
    "text",
    "lead_function",
]


def load_catalog(entity_name):
    root = os.path.dirname(os.path.realpath(__file__))
    path = os.path.join(root, 'catalog/{}.json'.format(entity_name))
    with open(path, "r") as f:
        data = json.load(f)

    return CatalogEntry(**data)


def get_schema_for_type(type):
    if type in ["datetime", "date"]:
        return Schema(type=["string", "null"], format="date-time")

    elif type in ["integer", "percent", "score"]:
        return Schema(type=["integer", "null"])

    elif type in ["float", "currency"]:
        return Schema(type=["number", "null"])

    elif type == "boolean":
        return Schema(type=["boolean", "null"])

    elif type in STRING_TYPES:
        return Schema(type=["string", "null"])

    return None


def discover_catalog(entity_name, client):
    data = client.request("rest/v1/{}/describe.json".format(entity_name)).json()
    data = data["result"][0]

    properties = {}
    for field in data["fields"]:
        field_schema = get_schema_for_type(field["dataType"])
        if field_schema:
            properties[field["name"]] = field_schema
        else:
            LOGGER.info("Unable to determine schema for %s with type %s", field["name"], field["dataType"])

    entity = CatalogEntry(
        tap_stream_id=entity_name,
        stream=entity_name,
        key_properties=data["idField"],
        schema=Schema(type="object", properties=properties),
    )

    if "updatedAt" in schema.properties:
        entity.replication_key = "updatedAt"
    elif "createdAt" in schema.properties:
        entity.replication_key = "createdAt"

    return entity


def discover_entities(client):
    entities = Catalog()

    for entity_name, action in ENTITIES_ACTIONS:
        if action == LOAD:
            entity = load_catalog(entity_name)
        elif action == DISCOVER:
            entity = discover_catalog(entity_name, client)

        entities.add_stream(entity)

    return entities
