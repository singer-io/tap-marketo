import json
import os
import sys

import singer
from singer import metadata

STRING_TYPES = [
    'string',
    'email',
    'reference',
    'url',
    'phone',
    'textarea',
    'text',
    'lead_function',
]


def clean_string(string):
    return string.lower().replace(" ", "_")


def get_schema_for_type(typ, null=False):
    # http://developers.marketo.com/rest-api/lead-database/fields/field-types/
    if typ in ['datetime', 'date']:
        rtn = {"type": "string", "format": "date-time"}
    elif typ in ['integer', 'percent', 'score']:
        rtn = {'type': 'integer'}
    elif typ in ['float', 'currency']:
        rtn = {'type': 'number'}
    elif typ == 'boolean':
        rtn = {'type': 'boolean'}
    elif typ in STRING_TYPES:
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
    # Activity streams have 6 attributes:
    # - marketoGUID
    # - leadId
    # - activityDate
    # - activityTypeId
    # - primaryAttribute
    # - attributes
    #
    # marketoGUID, leadId, activityDate, and activityTypeId are simple
    # fields. primaryAttribute has a name and type which define an
    # automatically included field on the record. Attributes is an array
    # of attribute names and types that become available fields.

    # Regarding pimaryAttribute fields: On this side of things, Marketo will
    # describe the field in an activity that is considered the primary attribute
    # On the sync side, we will have to present that information in a flattened record
    properties = {
        "marketoGUID": {"type": "string", "inclusion": "automatic"},
        "leadId": {"type": "integer", "inclusion": "automatic"},
        "activityDate": {"type": "string", "format": "date-time", "inclusion": "automatic"},
        "activityTypeId": {"type": "integer", "inclusion": "automatic"},
        "primaryAttributeValue": {"type": "string", "inclusion": "automatic"},
        "primaryAttributeName": {"type": "string", "inclusion": "automatic"},
        "primaryAttributeValueId": {"type": "string", "inclusion": "automatic"},    
    }

    mdata = metadata.new()
    
    if "primaryAttribute" in activity:
        primary = clean_string(activity["primaryAttribute"]["name"])
        mdata = metadata.write(mdata, (), 'primary_attribute_name', primary)

    if "attributes" in activity:
        for attr in activity["attributes"]:
            attr_name = clean_string(attr["name"])
            field_schema = get_schema_for_type(attr["dataType"], null=True)
            if field_schema:
                properties[attr_name] = field_schema

    activity_type_camel = clean_string(activity["name"])
    mdata = metadata.write(mdata, (), 'activity_id', activity["id"])

    tap_stream_id = "activities_{}".format(activity_type_camel)
    
    return {
        "tap_stream_id": tap_stream_id,
        "stream": tap_stream_id,
        "key_properties": ["marketoGUID"],
        "replication_key": "activityDate",
        "replication_method": "INCREMENTAL",
        "metadata": metadata.to_list(mdata),
        "schema": {
            "type": "object",
            "additionalProperties": False,
            "inclusion": "available",
            "properties": properties,
        },
    }


def discover_activities(client):
    # http://developers.marketo.com/rest-api/lead-database/activities/#describe
    endpoint = "rest/v1/activities/types.json"
    data = client.request("GET", endpoint, endpoint_name="activity_types")
    return [get_activity_type_stream(row) for row in data["result"]]


def discover_leads(client):
    # http://developers.marketo.com/rest-api/lead-database/leads/#describe
    endpoint = "rest/v1/leads/describe.json"
    data = client.request("GET", endpoint, endpoint_name="leads_discover")
    properties = {}
    for field in data["result"]:
        if "rest" not in field:
            singer.log_debug("Field leads.%s not supported via the REST API.",
                             field["displayName"])
            continue

        if field["rest"]["name"] == "id":
            field_schema = get_schema_for_type(field["dataType"], null=False)
        else:
            field_schema = get_schema_for_type(field["dataType"], null=True)

        if not field_schema:
            singer.log_debug("Marketo type %s unsupported for leads.%s",
                             field["dataType"], field["rest"]["name"])
            continue

        properties[field["rest"]["name"]] = field_schema

    return {
        "tap_stream_id": "leads",
        "stream": "leads",
        "key_properties": ["id"],
        "replication_key": "updatedAt",
        "replication_method": "INCREMENTAL",
        "schema": {
            "type": "object",
            "additionalProperties": False,
            "inclusion": "available",
            "properties": properties,
        },
    }


def discover_catalog(name):
    root = os.path.dirname(os.path.realpath(__file__))
    path = os.path.join(root, 'schemas/{}.json'.format(name))
    with open(path, "r") as f:
        return json.load(f)


def discover(client):
    singer.log_info("Starting discover")
    streams = []
    streams.append(discover_leads(client))
    streams.append(discover_catalog("activity_types"))
    streams.extend(discover_activities(client))
    streams.append(discover_catalog("campaigns"))
    streams.append(discover_catalog("lists"))
    streams.append(discover_catalog("programs"))
    json.dump({"streams": streams}, sys.stdout, indent=2)
    singer.log_info("Finished discover")
