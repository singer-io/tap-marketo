import json
import os

import singer
from singer import metadata
from tap_marketo.client import MarketoForbiddenError
from tap_marketo.sync import determine_replication_key


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


def _stream_replication_metadata(tap_stream_id):
    replication_key = determine_replication_key(tap_stream_id)
    replication_method = "INCREMENTAL" if replication_key else "FULL_TABLE"
    return replication_method, replication_key


def build_stream_entry(
        tap_stream_id,
        key_properties,
        schema,
        mdata,
        parent_stream=None):
    replication_method, replication_key = _stream_replication_metadata(tap_stream_id)
    if metadata.get(mdata, (), "inclusion") is None:
        mdata = metadata.write(mdata, (), "inclusion", "available")
    if parent_stream:
        mdata = metadata.write(mdata, (), "parent-tap-stream-id", parent_stream)
    return {
        "tap_stream_id": tap_stream_id,
        "stream": tap_stream_id,
        "key_properties": key_properties,
        "metadata": metadata.to_list(mdata),
        "schema": schema,
        "replication_method": replication_method,
        "replication_key": replication_key,
        "parent_stream": parent_stream,
    }

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


def set_replication_metadata(mdata, valid_replication_keys):
    mdata = metadata.write(mdata, (), 'forced-replication-method', 'FULL_TABLE')
    if valid_replication_keys:
        if not isinstance(valid_replication_keys, list):
            valid_replication_keys = [valid_replication_keys]
        mdata = metadata.write(mdata, (), 'forced-replication-method', 'INCREMENTAL')
        mdata = metadata.write(mdata, (), 'valid-replication-keys', valid_replication_keys)
    return mdata


def get_activity_type_stream(activity):
    # Activity streams have 7 attributes:
    # - marketoGUID
    # - leadId
    # - activityDate
    # - activityTypeId
    # - primaryAttribute
    # - attributes
    # - campaignId
    #
    # marketoGUID, leadId, activityDate, activityTypeId, and campaignId are simple
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
        "activityTypeId": {"type": ["null", "integer"]},
        "campaignId": {"type": ["null", "integer"]},
    }

    for prop in properties:
        # Not every activity will have an associated campaignId, hence the option to select.
        if prop == "campaignId":
            mdata = metadata.write(mdata, ('properties', prop), 'inclusion', 'available')
        else:
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
    mdata = set_replication_metadata(
        mdata,
        valid_replication_keys=determine_replication_key('activities')
    )

    return build_stream_entry(
        tap_stream_id=tap_stream_id,
        key_properties=["marketoGUID"],
        schema={
            "type": "object",
            "additionalProperties": False,
            "properties": properties,
        },
        mdata=mdata,
        parent_stream="activity_types",
    )


def discover_activities(client):
    # http://developers.marketo.com/rest-api/lead-database/activities/#describe
    endpoint = "rest/v1/activities/types.json"
    try:
        data = client.request("GET", endpoint, endpoint_name="activity_types")
        return [get_activity_type_stream(row) for row in data["result"]]
    except MarketoForbiddenError as ex:
        singer.log_warning(
            "Excluding unauthorized stream '%s' from catalog. HTTP-Error-Message: '%s'",
            "activity_types",
            str(ex)
        )
        return None


def discover_leads(client):
    # http://developers.marketo.com/rest-api/lead-database/leads/#describe
    endpoint = "rest/v1/leads/describe.json"
    try:
        data = client.request("GET", endpoint, endpoint_name="leads_discover")
    except MarketoForbiddenError as ex:
        singer.log_warning(
            "Excluding unauthorized stream '%s' from catalog. HTTP-Error-Message: '%s'",
            "leads",
            str(ex)
        )
        return None

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
    mdata = set_replication_metadata(
        mdata,
        valid_replication_keys=determine_replication_key('leads')
    )

    return build_stream_entry(
        tap_stream_id="leads",
        key_properties=["id"],
        schema={
            "type": "object",
            "additionalProperties": False,
            "properties": properties,
        },
        mdata=mdata,
    )


def discover_catalog(name, automatic_inclusion, client=None, **kwargs):
    unsupported = kwargs.get("unsupported", frozenset([]))
    root = os.path.dirname(os.path.realpath(__file__))
    path = os.path.join(root, 'schemas/{}.json'.format(name))
    mdata = metadata.new()

    with open(path, "r") as f:
        discovered_schema = json.load(f)

        if "schema" in discovered_schema:
            tap_stream_id = discovered_schema.get("tap_stream_id", name)
            stream_schema = discovered_schema["schema"]
            key_properties = discovered_schema.get("key_properties", ["id"])
        else:
            tap_stream_id = name
            stream_schema = discovered_schema
            key_properties = ["id"]

        for field in stream_schema["properties"]:
            if field in automatic_inclusion:
                mdata = metadata.write(mdata, ('properties', field), 'inclusion', 'automatic')
            elif field in unsupported:
                mdata = metadata.write(mdata, ('properties', field), 'inclusion', 'unsupported')
            else:
                mdata = metadata.write(mdata, ('properties', field), 'inclusion', 'available')

        # The steams using discover_catalog all use "id" as the key_properties
        mdata = metadata.write(mdata, (), 'table-key-properties', key_properties)
        mdata = set_replication_metadata(
            mdata,
            determine_replication_key(tap_stream_id)
        )

        if client and not check_stream_access(client, tap_stream_id):
            return None

        return build_stream_entry(
            tap_stream_id=tap_stream_id,
            key_properties=key_properties,
            schema=stream_schema,
            mdata=mdata,
            parent_stream=None,
        )


# ---------------------------------------------------------------------------
# Stream access checks
# ---------------------------------------------------------------------------

# Mapping of tap_stream_id to the probe endpoint used to verify access.
# Activity type streams are probed via the shared activity_types endpoint.
STREAM_PROBE_ENDPOINTS = {
    "leads": ("GET", "rest/v1/leads/describe.json"),
    "activity_types": ("GET", "rest/v1/activities/types.json"),
    "campaigns": ("GET", "rest/v1/campaigns.json"),
    "lists": ("GET", "rest/v1/lists.json"),
    "programs": ("GET", "rest/asset/v1/programs.json"),
}


def _get_probe_key(stream_name):
    return "activity_types" if stream_name.startswith("activities_") else stream_name


def check_stream_access(client, stream_name) -> bool:
    """Probe stream_name's endpoint and return whether the credentials have read access.
    Returns False if a MarketoForbiddenError is raised; True otherwise.
    Activity sub-streams (activities_*) delegate to the shared 'activity_types' probe.
    """
    probe_key = _get_probe_key(stream_name)
    probe = STREAM_PROBE_ENDPOINTS.get(probe_key)
    if probe is None:
        # Unknown stream — assume accessible rather than blocking discovery.
        return True

    method, endpoint = probe
    try:
        client.request(method, endpoint, endpoint_name="{}_access_check".format(probe_key))
        return True
    except MarketoForbiddenError as ex:
        singer.log_warning(
            "Excluding unauthorized stream '%s' from catalog. HTTP-Error-Message: '%s'",
            stream_name,
            str(ex)
        )
        return False


def discover(client):
    singer.log_info("Starting discover")
    streams = []
    inaccessible_streams = []

    leads_stream = discover_leads(client)
    if leads_stream:
        streams.append(leads_stream)
    else:
        inaccessible_streams.append("leads")

    activity_streams = discover_activities(client)
    if activity_streams is not None:
        activity_types_stream = discover_catalog(
            "activity_types",
            ACTIVITY_TYPES_AUTOMATIC_INCLUSION,
            unsupported=ACTIVITY_TYPES_UNSUPPORTED,
            client=client,
        )
        if activity_types_stream:
            streams.append(activity_types_stream)
            streams.extend(activity_streams)
        else:
            inaccessible_streams.append("activity_types")
            for stream in activity_streams:
                child_stream_id = stream["tap_stream_id"]
                singer.log_warning(
                    "Excluding child stream '%s' from catalog because parent stream '%s' is not accessible.",
                    child_stream_id,
                    "activity_types",
                )
                inaccessible_streams.append(child_stream_id)
    else:
        inaccessible_streams.append("activity_types")

    campaigns_stream = discover_catalog("campaigns", CAMPAIGNS_AUTOMATIC_INCLUSION, client=client)
    if campaigns_stream:
        streams.append(campaigns_stream)
    else:
        inaccessible_streams.append("campaigns")

    lists_stream = discover_catalog("lists", LISTS_AUTOMATIC_INCLUSION, client=client)
    if lists_stream:
        streams.append(lists_stream)
    else:
        inaccessible_streams.append("lists")

    programs_stream = discover_catalog("programs", PROGRAMS_AUTOMATIC_INCLUSION, client=client)
    if programs_stream:
        streams.append(programs_stream)
    else:
        inaccessible_streams.append("programs")

    if not streams:
        raise MarketoForbiddenError(
            "The credentials do not have 'read' access to any supported streams."
        )

    if inaccessible_streams:
        singer.log_warning(
            "Unauthorized streams excluded from catalog: %s",
            ", ".join(inaccessible_streams),
        )

    catalog = {"streams": streams}
    singer.log_info("Finished discover")
    return catalog
