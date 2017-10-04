import json
import os

import singer

LOGGER = singer.get_logger()


def clean_string(string):
    return string.lower().replace(" ", "_")


def get_schema_for_type(typ, null=False):
    if typ in ['datetime', 'date']:
        rtn = {"type": "string", "format": "date-time"}
    elif typ in ['integer', 'percent', 'score']:
        rtn = {'type': 'integer'}
    elif typ in ['float', 'currency']:
        rtn = {'type': 'number'}
    elif typ == 'boolean':
        rtn = {'type': 'boolean'}
    elif typ in ['string', 'email', 'reference', 'url', 'phone', 'textarea', 'text', 'lead_function']:
        rtn = {'type': 'string'}
    else:
        return None

    if null:
        rtn["type"] = [rtn["type"], "null"]
        rtn["inclusion"] = "available"
    else:
        rtn["inclusion"] = "automatic"

    return rtn


def get_activity_type_stream(activity):
    properties = {
        "marketoGUID": {"type": "string", "inclusion": "automatic"},
        "leadId": {"type": "integer", "inclusion": "automatic"},
        "activityDate": {"type": "string", "format": "date-time", "inclusion": "automatic"},
        "activityTypeId": {"type": "integer", "inclusion": "automatic"},
    }

    primary = clean_string(activity["primaryAttribute"]["name"])
    properties[primary] = get_schema_for_type(activity["primaryAttribute"]["type"], null=False)
    properties[primary + "_id"] = get_schema_for_type("integer", null=False)

    for attr in activity["attributes"]:
        attr_name = clean_string(attr["name"])
        properties[attr_name] = get_schema_for_type(attr["type"], null=True)

    tap_stream_id = "activities_{}".format(activity["id"])
    return {
        "tap_stream_id": tap_stream_id,
        "stream": tap_stream_id,
        "key_properties": ["marketoGUID"],
        "replication_key": "activityDate",
        "schema": {
            "type": "object",
            "additionalProperties": False,
            "properties": properties,
        },
    }


def discover_activities(client):
    endpoint = "rest/v1/activities/types.json"
    data = client.request("GET", endpoint)
    return [get_activity_type_stream(row) for row in data["result"]]


def discover_leads(client):
    data = client.request("rest/v1/leads/describe.json")
    properties = {}
    for field in data["result"][0]["fields"]:
        if field["name"] == "id":
            field_schema = get_schema_for_type(field["dataType"], null=False)
        else:
            field_schema = get_schema_for_type(field["dataType"], null=True)

        if not field_schema:
            continue

        properties[field["name"]] = field_schema

    return {
        "tap_stream_id": "leads",
        "stream": "leads",
        "key_properties": ["id"],
        "replication_key": "updatedAt",
        "schema": {
            "type": "object",
            "additionalProperties": False,
            "properties": properties,
        },
    }

def discover_catalog(name):
    root = os.path.dirname(os.path.realpath(__file__))
    path = os.path.join(root, 'catalog/{}.json'.format(name))
    with open(path, "r") as f:
        return json.load(f)


def discover(client):
    LOGGER.info("Starting discover")

    streams = []
    streams.append(discover_leads(client))
    streams.append(discover_catalog("activity_types"))
    streams.extend(discover_activities(client))
    streams.append(discover_catalog("campaigns"))
    streams.append(discover_catalog("lists"))
    streams.append(discover_catalog("programs"))

    LOGGER.info("Finished discover")
    return streams
