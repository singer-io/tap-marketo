from datetime import timedelta

import csv
import json
import pendulum
import tempfile

import singer
from singer.catalog import Catalog
from singer import metadata
from singer import bookmarks
from singer import utils
from tap_marketo.client import ExportFailed, ApiQuotaExceeded

# We can request up to 30 days worth of activities per export.
MAX_EXPORT_DAYS = 30

BASE_ACTIVITY_FIELDS = [
    "marketoGUID",
    "leadId",
    "activityDate",
    "activityTypeId",
    "campaignId",
]

ACTIVITY_FIELDS = BASE_ACTIVITY_FIELDS + [
    "primaryAttributeValue",
    "primaryAttributeValueId",
    "attributes",
]


STREAMS_WITH_UPDATED_AT_REPLICATION_KEY = {
    "leads",
    "lists",
    "campaigns",
    "programs",
    "emails",
}


def determine_replication_key(tap_stream_id):
    if tap_stream_id.startswith("activities_"):
        return 'activityDate'
    if tap_stream_id == 'activity_types':
        return None
    if tap_stream_id in STREAMS_WITH_UPDATED_AT_REPLICATION_KEY:
        return 'updatedAt'
    return None


NO_ASSET_MSG = "No assets found for the given search criteria."
NO_CORONA_WARNING = (
    "Your account does not have Corona support enabled. Without Corona, each sync of "
    "the Leads table requires a full export which can lead to lower data freshness. "
    "Please contact Marketo to request Corona support be added to your account."
)
ITER_CHUNK_SIZE = 512

ATTRIBUTION_WINDOW_DAYS = 1


def format_value(value, _type):
    """
    Conform the field's value with its type (for json-schema basic types)

    NOTES: 
    * This function gets called on each field of each row, and is thus its performance
    is critical to processing the downloaded csv file.
    * This function does not handle object or array types, nor type unions which are 
      still valid jsonschemas (e.g. {"type": ["string", "integer"]})
    Returns any of:
        None
        datetime-string
        integer
        float
        boolean
        string
        compoud data structure, dict / list: default last option
    """
    if value is None or value == '' or value == 'null':
        return None
    if _type == "string":
        return str(value)
    if _type == "date-time":
        return pendulum.parse(value).isoformat()
    if _type == "integer":
        return int(float(value))
    if _type == "number":
        return float(value)
    if _type == "boolean":
        return str(value).lower() == "true"
    return value


def format_values(output_typemap, row):
    """
    Conform the row's values with their type signature and 
    filter out fields not included in the output record.

    `output_typemap`: dictionary with output fields and their types
    `row`: the data record.
    """
    return {
        field: format_value(row.get(field), field_type)
        for field, field_type in output_typemap.items()
    }


def get_stream_fields(stream):
    """Get the list of fields selected in the stream."""
    fields = [
        entry['breadcrumb'][-1] for entry in stream['metadata']
        if len(entry['breadcrumb']) > 0 and (
            entry['metadata'].get('selected')
            or entry['metadata'].get('inclusion') == 'automatic')
    ]
    msg = ("following fields are selected in stream "
           f"{stream['tap_stream_id']}: {fields}")
    singer.log_info(msg)
    return fields


def update_state_with_export_info(state, stream, bookmark=None, export_id=None, export_end=None):
    state = bookmarks.write_bookmark(state, stream["tap_stream_id"], "export_id", export_id)
    state = bookmarks.write_bookmark(state, stream["tap_stream_id"], "export_end", export_end)
    if bookmark:
        state = bookmarks.write_bookmark(state, stream["tap_stream_id"], determine_replication_key(stream['tap_stream_id']), bookmark)

    singer.write_state(state)
    return state


def get_export_end(export_start, end_days=MAX_EXPORT_DAYS):
    export_end = export_start.add(days=end_days)
    if export_end >= pendulum.utcnow():
        export_end = pendulum.utcnow()

    return export_end.replace(microsecond=0)


def wait_for_export(client, state, stream, export_id):
    stream_type = "activities" if stream["tap_stream_id"] != "leads" else "leads"
    try:
        client.wait_for_export(stream_type, export_id)
    except ExportFailed:
        state = update_state_with_export_info(state, stream)
        raise

    return state

MEGABYTE_IN_BYTES = 1024 * 1024
CHUNK_SIZE_MB = 10
CHUNK_SIZE_BYTES = MEGABYTE_IN_BYTES * CHUNK_SIZE_MB

# This function has an issue with UTF-8 data most likely caused by decode_unicode=True
# See https://github.com/singer-io/tap-marketo/pull/51/files
def stream_rows(client, stream_type, export_id):
    with tempfile.NamedTemporaryFile(mode="w+", encoding="utf8") as csv_file:
        singer.log_info("Download starting.")
        resp = client.stream_export(stream_type, export_id)
        for chunk in resp.iter_content(chunk_size=CHUNK_SIZE_BYTES, decode_unicode=True):
            if chunk:
                # Replace CR
                chunk = chunk.replace('\r', '').replace('\0', '')
                csv_file.write(chunk)

        singer.log_info("Download completed. Begin streaming rows.")
        csv_file.seek(0)

        reader = csv.DictReader(csv_file, delimiter=',', quotechar='"')
        for line_dict in reader:
            yield line_dict


def get_or_create_export_for_leads(client, state, stream, export_start, config):
    export_id = bookmarks.get_bookmark(state, "leads", "export_id")
    # check if export is still valid
    if export_id is not None and not client.export_available("leads", export_id):
        singer.log_info("Export %s no longer available.", export_id)
        export_id = None

    if export_id is None:
        # Corona mode is required to query by "updatedAt", otherwise a full
        # sync is required using "createdAt".
        query_field = "updatedAt" if client.use_corona else "createdAt"
        max_export_days = int(config.get('max_export_days',
                                         MAX_EXPORT_DAYS))
        export_end = get_export_end(export_start,
                                    end_days=max_export_days)
        query = {query_field: {"startAt": export_start.isoformat(),
                               "endAt": export_end.isoformat()}}

        # Create the new export and store the id and end date in state.
        # Does not start the export (must POST to the "enqueue" endpoint).
        fields = get_stream_fields(stream)

        export_id = client.create_export("leads", fields, query)
        state = update_state_with_export_info(
            state, stream, export_id=export_id, export_end=export_end.isoformat())
    else:
        export_end = pendulum.parse(bookmarks.get_bookmark(state, "leads", "export_end"))

    return export_id, export_end


def get_or_create_export_for_activities(client, state, stream, export_start, config):
    export_id = bookmarks.get_bookmark(state, stream["tap_stream_id"], "export_id")
    if export_id is not None and not client.export_available("activities", export_id):
        singer.log_info("Export %s no longer available.", export_id)
        export_id = None

    if export_id is None:
        # The activity id is in the top-most breadcrumb of the metatdata
        # Activity ids correspond to activity type id in Marketo.
        # We need the activity type id to build the query.
        activity_metadata = metadata.to_map(stream["metadata"])
        activity_type_id = metadata.get(activity_metadata, (), 'marketo.activity-id')

        # Activities must be queried by `createdAt` even though
        # that is not a real field. `createdAt` proxies `activityDate`.
        # The activity type id must also be included in the query. The
        # largest date range that can be used for activities is 30 days.
        max_export_days = int(config.get('max_export_days',
                                         MAX_EXPORT_DAYS))
        export_end = get_export_end(export_start,
                                    end_days=max_export_days)
        query = {"createdAt": {"startAt": export_start.isoformat(),
                               "endAt": export_end.isoformat()},
                 "activityTypeIds": [activity_type_id]}

        # Create the new export and store the id and end date in state.
        # Does not start the export (must POST to the "enqueue" endpoint).
        try:
            export_id = client.create_export("activities", ACTIVITY_FIELDS, query)
        except ApiQuotaExceeded as e:
            # The main reason we wrap the ApiQuotaExceeded exception in a
            # new one is to be able to tell the customer what their
            # configured max_export_days is.
            raise ApiQuotaExceeded(
                ("You may wish to consider changing the "
                 "`max_export_days` config value to a lower number if "
                 "you're unable to sync a single {} day window within "
                 "your current API quota.").format(
                     max_export_days)) from e
        state = update_state_with_export_info(
            state, stream, export_id=export_id, export_end=export_end.isoformat())
    else:
        export_end = pendulum.parse(bookmarks.get_bookmark(state, stream["tap_stream_id"], "export_end"))

    return export_id, export_end


def flatten_activity(row, stream):
    # Start with the base fields
    rtn = {field: row[field] for field in BASE_ACTIVITY_FIELDS}

    # Add the primary attribute name
    # This name is the human readable name/description of the
    # pimaryAttribute
    mdata = metadata.to_map(stream['metadata'])
    pan_field = metadata.get(mdata, (), 'marketo.primary-attribute-name')
    if pan_field:
        rtn['primary_attribute_name'] = pan_field
        rtn['primary_attribute_value'] = row['primaryAttributeValue']
        rtn['primary_attribute_value_id'] = row['primaryAttributeValueId']

    # Now flatten the attrs json to it's selected columns
    if "attributes" in row:
        attrs = json.loads(row["attributes"])
        for key, value in attrs.items():
            key = key.lower().replace(" ", "_")
            rtn[key] = value

    return rtn


def get_output_typemap(stream):
    """
    Generate dictionary of {field: output_type} defined by the stream

    field is a string (the field name),
    and output_type is hashable, either string or tuple of strings and excludes `null`

    The output from this function is used
    """
    available_fields = set(get_stream_fields(stream))
    output_typemap = {}
    for field, prop in stream["schema"]["properties"].items():
        if field in available_fields:
            if prop.get("format") == "date-time":
                _type = "date-time"
            elif isinstance(prop["type"], str):
                _type = prop["type"]
            elif isinstance(prop["type"], list):
                not_null_type = [t for t in prop["type"] if t != "null"]
                if len(not_null_type) == 1:
                    _type = not_null_type[0]
                else:
                    _type = tuple(sorted(not_null_type))
            output_typemap[field] = _type
    return output_typemap


def sync_leads(client, state, stream, config):
    # http://developers.marketo.com/rest-api/bulk-extract/bulk-lead-extract/
    replication_key = determine_replication_key(stream["tap_stream_id"])
    output_typemap = get_output_typemap(stream)
    singer.write_schema("leads", stream["schema"], stream["key_properties"], bookmark_properties=[replication_key])
    initial_bookmark = pendulum.parse(bookmarks.get_bookmark(state, "leads", replication_key))
    export_start = pendulum.parse(bookmarks.get_bookmark(state, "leads", replication_key))
    if client.use_corona:
        aw = config.get(
            'attribution_window',
            timedelta(days=ATTRIBUTION_WINDOW_DAYS)
        )
        export_start = export_start.subtract(days=aw.days, seconds=aw.seconds)
    
    # job_started is truncated to the microsecond 
    # in get_export_end the field export_end is also truncated to the microsecond
    # export_start is set to export_end at the end of the following while loop
    # which results in an extra marketo export if job_started is not truncated
    job_started = pendulum.utcnow().replace(microsecond=0)
    record_count = 0
    max_bookmark = initial_bookmark
    while export_start < job_started:
        export_id, export_end = get_or_create_export_for_leads(client, state, stream, export_start, config)
        state = wait_for_export(client, state, stream, export_id)
        for row in stream_rows(client, "leads", export_id):
            time_extracted = utils.now()

            record = format_values(output_typemap, row)
            
            singer.write_record("leads", record, time_extracted=time_extracted)
            record_count += 1
            if not client.use_corona:
                record_bookmark = pendulum.parse(record[replication_key])
                if record_bookmark >= initial_bookmark:
                    max_bookmark = max(max_bookmark, record_bookmark)

        if client.use_corona:
            max_bookmark = export_end
        # Now that one of the exports is finished, update the bookmark
        state = update_state_with_export_info(state, stream, bookmark=max_bookmark.isoformat())
        export_start = export_end

    return state, record_count


def sync_activities(client, state, stream, config):
    # http://developers.marketo.com/rest-api/bulk-extract/bulk-activity-extract/
    replication_key = determine_replication_key(stream['tap_stream_id'])
    replication_key = determine_replication_key(stream['tap_stream_id'])
    output_typemap = get_output_typemap(stream)
    singer.write_schema(stream["tap_stream_id"], stream["schema"], stream["key_properties"], bookmark_properties=[replication_key])
    export_start = pendulum.parse(bookmarks.get_bookmark(state, stream["tap_stream_id"], replication_key))
    # job_started is truncated to the microsecond 
    # in get_export_end the field export_end is also truncated to the microsecond
    # export_start is set to export_end at the end of the following while loop
    # which results in an extra marketo export if job_started is not truncated
    job_started = pendulum.utcnow().replace(microsecond=0)
    record_count = 0
    while export_start < job_started:
        export_id, export_end = get_or_create_export_for_activities(client, state, stream, export_start, config)
        state = wait_for_export(client, state, stream, export_id)
        for row in stream_rows(client, "activities", export_id):
            time_extracted = utils.now()

            row = flatten_activity(row, stream)
            record = format_values(output_typemap, row)

            singer.write_record(stream["tap_stream_id"], record, time_extracted=time_extracted)
            record_count += 1

        state = update_state_with_export_info(state, stream, bookmark=export_start.isoformat())
        export_start = export_end

    return state, record_count


def sync_programs(client, state, stream):
    # http://developers.marketo.com/rest-api/assets/programs/#by_date_range
    #
    # Programs are queryable via their updatedAt time but require and
    # end date as well. As there is no max time range for the query,
    # query from the bookmark value until current.
    #
    # The Programs endpoint uses offsets with a return limit of 200
    # per page. If requesting past the final program, an error message
    # is returned to indicate that the endpoint has been fully synced.
    replication_key = determine_replication_key(stream['tap_stream_id'])
    output_typemap = get_output_typemap(stream)
    singer.write_schema("programs", stream["schema"], stream["key_properties"], bookmark_properties=[replication_key])
    start_date = bookmarks.get_bookmark(state, "programs", replication_key)
    end_date = pendulum.utcnow().isoformat()
    params = {
        "maxReturn": 200,
        "offset": 0,
        "earliestUpdatedAt": start_date,
        "latestUpdatedAt": end_date,
    }
    endpoint = "rest/asset/v1/programs.json"

    record_count = 0
    while True:
        data = client.request("GET", endpoint, endpoint_name="programs", params=params)

        # If the no asset message is in the warnings, we have exhausted
        # the search results and can end the sync.
        if "warnings" in data and NO_ASSET_MSG in data["warnings"]:
            break

        time_extracted = utils.now()

        # Each row just needs the values formatted. If the record is
        # newer than the original start date, stream the record.
        for row in data["result"]:
            record = format_values(output_typemap, row)
            if record[replication_key] >= start_date:
                record_count += 1

                singer.write_record("programs", record, time_extracted=time_extracted)

        # Increment the offset by the return limit for the next query.
        params["offset"] += params["maxReturn"]

    # Now that we've finished every page we can update the bookmark to
    # the end of the query.
    state = bookmarks.write_bookmark(state, "programs", replication_key, end_date)
    singer.write_state(state)
    return state, record_count


def sync_paginated(client, state, stream):
    # http://developers.marketo.com/rest-api/endpoint-reference/lead-database-endpoint-reference/#!/Campaigns/getCampaignsUsingGET
    # http://developers.marketo.com/rest-api/endpoint-reference/lead-database-endpoint-reference/#!/Static_Lists/getListsUsingGET
    #
    # Campaigns and Static Lists are paginated with a max return of 300
    # items per page. There are no filters that can be used to only
    # return updated records.
    replication_key = determine_replication_key(stream['tap_stream_id'])
    output_typemap = get_output_typemap(stream)
    singer.write_schema(stream["tap_stream_id"], stream["schema"], stream["key_properties"], bookmark_properties=[replication_key])
    start_date = bookmarks.get_bookmark(state, stream["tap_stream_id"], replication_key)
    params = {"batchSize": 300}
    endpoint = "rest/v1/{}.json".format(stream["tap_stream_id"])

    # Paginated requests use paging tokens for retrieving the next page
    # of results. These tokens are stored in the state for resuming
    # syncs. If a paging token exists in state, use it.
    next_page_token = bookmarks.get_bookmark(state, stream["tap_stream_id"], "next_page_token")
    if next_page_token:
        params["nextPageToken"] = next_page_token

    # Keep querying pages of data until no next page token.
    record_count = 0
    job_started = pendulum.utcnow().isoformat()
    while True:
        data = client.request("GET", endpoint, endpoint_name=stream["tap_stream_id"], params=params)

        time_extracted = utils.now()

        # Each row just needs the values formatted. If the record is
        # newer than the original start date, stream the record. Finally,
        # update the bookmark if newer than the existing bookmark.
        for row in data["result"]:
            record = format_values(output_typemap, row)
            if record[replication_key] >= start_date:
                record_count += 1

                singer.write_record(stream["tap_stream_id"], record, time_extracted=time_extracted)

        # No next page, results are exhausted.
        if "nextPageToken" not in data:
            break

        # Store the next page token in state and continue.
        params["nextPageToken"] = data["nextPageToken"]
        state = bookmarks.write_bookmark(state, stream["tap_stream_id"], "next_page_token", data["nextPageToken"])
        singer.write_state(state)

    # Once all results are exhausted, unset the next page token bookmark
    # so the subsequent sync starts from the beginning.
    state = bookmarks.write_bookmark(state, stream["tap_stream_id"], "next_page_token", None)
    state = bookmarks.write_bookmark(state, stream["tap_stream_id"], replication_key, job_started)
    singer.write_state(state)
    return state, record_count


def sync_activity_types(client, state, stream):
    # http://developers.marketo.com/rest-api/lead-database/activities/#describe
    #
    # Activity types aren't even paginated. Grab all the results in one
    # request, format the values, and output them.
    output_typemap = get_output_typemap(stream)
    singer.write_schema("activity_types", stream["schema"], stream["key_properties"])
    endpoint = "rest/v1/activities/types.json"
    data = client.request("GET", endpoint, endpoint_name="activity_types")
    record_count = 0

    time_extracted = utils.now()

    for row in data["result"]:
        record = format_values(output_typemap, row)
        record_count += 1

        singer.write_record("activity_types", record, time_extracted=time_extracted)

    return state, record_count


def sync(client, catalog, config, state):
    starting_stream = bookmarks.get_currently_syncing(state)
    if starting_stream:
        singer.log_info("Resuming sync from %s", starting_stream)
    else:
        singer.log_info("Starting sync")

    if isinstance(catalog, Catalog):
        catalog = catalog.to_dict()
    for stream in catalog["streams"]:
        # Skip unselected streams.
        mdata = metadata.to_map(stream['metadata'])

        if not metadata.get(mdata, (), 'selected'):
            singer.log_info("%s: not selected", stream["tap_stream_id"])
            continue

        # Skip streams that have already be synced when resuming.
        if starting_stream and stream["tap_stream_id"] != starting_stream:
            singer.log_info("%s: already synced", stream["tap_stream_id"])
            continue

        singer.log_info("%s: starting sync", stream["tap_stream_id"])

        # Now that we've started, there's no more "starting stream". Set
        # the current stream to resume on next run.
        starting_stream = None
        state = bookmarks.set_currently_syncing(state, stream["tap_stream_id"])
        singer.write_state(state)

        # Sync stream based on type.
        if stream["tap_stream_id"] == "activity_types":
            state, record_count = sync_activity_types(client, state, stream)
        elif stream["tap_stream_id"] == "leads":
            state, record_count = sync_leads(client, state, stream, config)
        elif stream["tap_stream_id"].startswith("activities_"):
            state, record_count = sync_activities(client, state, stream, config)
        elif stream["tap_stream_id"] in ["campaigns", "lists"]:
            state, record_count = sync_paginated(client, state, stream)
        elif stream["tap_stream_id"] == "programs":
            state, record_count = sync_programs(client, state, stream)
        else:
            raise Exception("Stream %s not implemented" % stream["tap_stream_id"])

        # Emit metric for record count.
        counter = singer.metrics.record_counter(stream["tap_stream_id"])
        counter.value = record_count
        counter._pop()  # pylint: disable=protected-access

        # Unset current stream.
        state = bookmarks.set_currently_syncing(state, None)
        singer.write_state(state)
        singer.log_info("%s: finished sync", stream["tap_stream_id"])

    # If Corona is not supported, log a warning near the end of the tap
    # log with instructions on how to get Corona supported.
    singer.log_info("Finished sync.")
    if not client.use_corona:
        singer.log_warning(NO_CORONA_WARNING)
