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

ACTIVITY_TYPES_AUTOMATIC_INCLUSION = frozenset(["id", "name"])
ACTIVITY_TYPES_UNSUPPORTED = frozenset(["attributes"])
LISTS_AUTOMATIC_INCLUSION = frozenset(["id", "name", "createdAt", "updatedAt"])
PROGRAMS_AUTOMATIC_INCLUSION = frozenset(["id", "createdAt", "updatedAt"])
CAMPAIGNS_AUTOMATIC_INCLUSION = frozenset(["id", "createdAt", "updatedAt"])

LEAD_REQUIRED_FIELDS = frozenset(["id", "updatedAt", "createdAt"])

def clean_string(string):
    return string.lower().replace(" ", "_")


def get_schema_for_type(typ, breadcrumb, mdata, null=False):
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
    elif typ in ['array']:
        rtn = {'type': 'array',
               'items': {'type': ['integer','number','string','null']}}
    else:
        rtn = {'type': 'string'}

    if null:
        rtn["type"] = [rtn["type"], "null"]
        mdata = metadata.write(mdata, breadcrumb, 'inclusion', 'available')

    else:
        mdata = metadata.write(mdata, breadcrumb, 'inclusion', 'automatic')

    return rtn, mdata


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
    mdata = metadata.new()

    properties = {
        "marketoGUID": {"type": ["null", "string"]},
        "leadId": {"type": ["null", "integer"]},
        "activityDate": {"type": ["null", "string"], "format": "date-time"},
        "activityTypeId": {"type": ["null", "integer"]}
    }

    for prop in properties:
        mdata = metadata.write(mdata, ('properties', prop), 'inclusion', 'automatic')

    if "primaryAttribute" in activity:
        properties["primary_attribute_value"] = {"type": ["null", "string"]}
        properties["primary_attribute_name"] = {"type": ["null", "string"]}
        properties["primary_attribute_value_id"] = {"type": ["null", "string"]}

        mdata = metadata.write(mdata, ('properties', "primary_attribute_value"), 'inclusion', 'automatic')
        mdata = metadata.write(mdata, ('properties', "primary_attribute_name"), 'inclusion', 'automatic')
        mdata = metadata.write(mdata, ('properties', "primary_attribute_value_id"), 'inclusion', 'automatic')


        primary = clean_string(activity["primaryAttribute"]["name"])
        mdata = metadata.write(mdata, (), 'marketo.primary-attribute-name', primary)


    if "attributes" in activity:
        for attr in activity["attributes"]:
            attr_name = clean_string(attr["name"])
            field_schema, mdata = get_schema_for_type(attr["dataType"], breadcrumb=('properties', attr_name), mdata=mdata, null=True)
            if field_schema:
                properties[attr_name] = field_schema

    activity_type_camel = clean_string(activity["name"])
    mdata = metadata.write(mdata, (), 'marketo.activity-id', activity["id"])

    tap_stream_id = "activities_{}".format(activity_type_camel)

    # The activities steams use "marketoGUID" as the key_properties
    mdata = metadata.write(mdata, (), 'table-key-properties', ['marketoGUID'])

    return {
        "tap_stream_id": tap_stream_id,
        "stream": tap_stream_id,
        "key_properties": ["marketoGUID"],
        "metadata": metadata.to_list(mdata),
        "schema": {
            "type": "object",
            "additionalProperties": False,
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
    mdata = metadata.new()

    for field in data["result"]:
        if "rest" not in field:
            singer.log_debug("Field leads.%s not supported via the REST API.",
                             field["displayName"])
            continue
        field_name = field["rest"]["name"]

        if field["rest"]["name"] in LEAD_REQUIRED_FIELDS:
            field_schema, mdata = get_schema_for_type(field["dataType"], ('properties', field_name), mdata, null=False)
        else:
            field_schema, mdata = get_schema_for_type(field["dataType"], ('properties', field_name), mdata, null=True)

        if not field_schema:
            singer.log_debug("Marketo type %s unsupported for leads.%s",
                             field["dataType"], field_name)
            continue
        properties[field_name] = field_schema

    # The leads steam uses "id" as the key_properties
    mdata = metadata.write(mdata, (), 'table-key-properties', ['id'])

    return {
        "tap_stream_id": "leads",
        "stream": "leads",
        "key_properties": ["id"],
        "metadata": metadata.to_list(mdata),
        "schema": {
            "type": "object",
            "additionalProperties": False,
            "properties": properties,
        },
    }


def discover_catalog(name, automatic_inclusion, **kwargs):
    unsupported = kwargs.get("unsupported", frozenset([]))
    stream_automatic_inclusion = kwargs.get("stream_automatic_inclusion", False)
    root = os.path.dirname(os.path.realpath(__file__))
    path = os.path.join(root, 'schemas/{}.json'.format(name))
    mdata = metadata.new()

    with open(path, "r") as f:
        discovered_schema = json.load(f)

        for field in discovered_schema["schema"]["properties"]:
            if field in automatic_inclusion:
                mdata = metadata.write(mdata, ('properties', field), 'inclusion', 'automatic')
            elif field in unsupported:
                mdata = metadata.write(mdata, ('properties', field), 'inclusion', 'unsupported')
            else:
                mdata = metadata.write(mdata, ('properties', field), 'inclusion', 'available')

        if stream_automatic_inclusion:
            mdata = metadata.write(mdata, (), 'inclusion', 'automatic')

        # The steams using discover_catalog all use "id" as the key_properties
        mdata = metadata.write(mdata, (), 'table-key-properties', ['id'])

        discovered_schema["metadata"] = metadata.to_list(mdata)
        return discovered_schema

def discover(client):
    singer.log_info("Starting discover")
    streams = []
    streams.append(discover_leads(client))
    streams.append(discover_catalog("activity_types", ACTIVITY_TYPES_AUTOMATIC_INCLUSION, unsupported=ACTIVITY_TYPES_UNSUPPORTED, stream_automatic_inclusion=True))
    streams.extend(discover_activities(client))
    streams.append(discover_catalog("campaigns", CAMPAIGNS_AUTOMATIC_INCLUSION))
    streams.append(discover_catalog("lists", LISTS_AUTOMATIC_INCLUSION))
    streams.append(discover_catalog("programs", PROGRAMS_AUTOMATIC_INCLUSION))
    json.dump({"streams": streams}, sys.stdout, indent=2)
    singer.log_info("Finished discover")
