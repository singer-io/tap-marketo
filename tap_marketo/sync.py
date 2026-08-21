import csv
import io
import json
import pendulum
from requests.exceptions import ChunkedEncodingError, ConnectionError
from urllib3.exceptions import ProtocolError

import singer
from singer import metadata
from singer import bookmarks
from singer import utils
from tap_marketo.client import utcnow, ExportFailed, ApiQuotaExceeded


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

def determine_replication_key(tap_stream_id):
    if tap_stream_id.startswith("activities_"):
        return 'activityDate'
    elif tap_stream_id == 'activity_types':
        return None
    elif tap_stream_id == 'leads':
        return 'updatedAt'
    elif tap_stream_id == 'lists':
        return 'updatedAt'
    elif tap_stream_id == 'campaigns':
        return 'updatedAt'
    elif tap_stream_id == 'programs':
        return 'updatedAt'
    else:
        return None


NO_ASSET_MSG = "No assets found for the given search criteria."
NO_CORONA_WARNING = (
    "Your account does not have Corona support enabled. Without Corona, each sync of "
    "the Leads table requires a full export which can lead to lower data freshness. "
    "Please contact Marketo to request Corona support be added to your account."
)
ITER_CHUNK_SIZE = 512

ATTRIBUTION_WINDOW_DAYS = 1


def format_value(value, schema):
    if not isinstance(schema["type"], list):
        field_type = [schema["type"]]
    else:
        field_type = schema["type"]

    if value in [None, "", 'null']:
        return None
    elif schema.get("format") == "date-time":
        return pendulum.parse(value).isoformat()
    elif "integer" in field_type:
        if isinstance(value, int):
            return value

        # Custom Marketo percent type fields can have decimals, so we drop them
        decimal_index = value.find('.')
        if decimal_index > 0:
            singer.log_warning("Dropping decimal from integer type. Original Value: %s", value)
            value = value[:decimal_index]
        return int(value)
    elif "string" in field_type:
        return str(value)
    elif "number" in field_type:
        return float(value)
    elif "boolean" in field_type:
        if isinstance(value, bool):
            return value
        return value.lower() == "true"

    return value


def get_available_fields(stream):
    """Return the set of selected/automatic field names for a stream.

    Computing this once per stream (rather than per row) avoids rebuilding the
    set on every row processed during a bulk export.
    """
    available_fields = set()
    for entry in stream['metadata']:
        if len(entry['breadcrumb']) > 0 and (
            entry['metadata'].get('selected') or
            entry['metadata'].get('inclusion') == 'automatic'
        ):
            available_fields.add(entry['breadcrumb'][-1])
    return available_fields


def format_values(stream, row, available_fields=None):
    if available_fields is None:
        available_fields = get_available_fields(stream)
    rtn = {}
    for field, schema in stream["schema"]["properties"].items():
        if field in available_fields:
            rtn[field] = format_value(row.get(field), schema)
    return rtn


def update_state_with_export_info(state, stream, bookmark=None, export_id=None, export_end=None):
    state = bookmarks.write_bookmark(state, stream["tap_stream_id"], "export_id", export_id)
    state = bookmarks.write_bookmark(state, stream["tap_stream_id"], "export_end", export_end)
    if bookmark:
        state = bookmarks.write_bookmark(state, stream["tap_stream_id"], determine_replication_key(stream['tap_stream_id']), bookmark)

    singer.write_state(state)
    return state


def get_export_end(export_start, end_days=MAX_EXPORT_DAYS):
    export_end = export_start.add(days=end_days)
    current_time = utcnow()
    if export_end >= current_time:
        export_end = current_time

    return export_end.replace(microsecond=0)


# Marketo's bulk extract API enforces a daily data-volume quota. A single
# wide export window can produce a file too large to extract, which fails
# with ApiQuotaExceeded (1029) and returns no data at all. When that happens
# we halve the window and retry so we can still make forward progress, down
# to a floor of MIN_EXPORT_DAYS. If even the smallest window exceeds quota we
# re-raise so downstream processes see the failure.
MIN_EXPORT_DAYS = 2


def create_export_with_quota_backoff(create_fn, export_start, max_export_days):
    """Create a bulk export for the window starting at ``export_start``.

    ``create_fn`` is called with the chosen ``export_end`` and must return the
    new export id. On ApiQuotaExceeded we halve the window (bounded below by
    MIN_EXPORT_DAYS) and retry; if the minimum window still exceeds quota the
    error is re-raised. Returns ``(export_id, export_end)``.
    """
    export_end = get_export_end(export_start, end_days=max_export_days)
    while True:
        try:
            export_id = create_fn(export_end)
            return export_id, export_end
        except ApiQuotaExceeded as e:
            # in_days() floors to whole days, matching how we size windows.
            window_days = (export_end - export_start).in_days()
            if window_days <= MIN_EXPORT_DAYS:
                raise e
            new_days = max(MIN_EXPORT_DAYS, window_days // 2)
            singer.log_warning(
                "Hit Marketo API quota creating export; retrying with a "
                "smaller %s-day window (was %s days).", new_days, window_days)
            export_end = get_export_end(export_start, end_days=new_days)


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

class IterStream(io.RawIOBase):
    """Adapts a byte-chunk iterator into a file-like object for io.BufferedReader/TextIOWrapper."""

    def __init__(self, iterator):
        self._iter = iterator
        self._leftover = b''

    def readable(self):
        return True

    def readinto(self, buf):
        try:
            chunk = self._leftover or next(self._iter)
            out, self._leftover = chunk[:len(buf)], chunk[len(buf):]
            buf[:len(out)] = out
            return len(out)
        except StopIteration:
            return 0
        except (ChunkedEncodingError, ConnectionError, BrokenPipeError, ProtocolError):
            # Preserve original traceback for upstream handling.
            raise

MAX_EMPTY_RESUMES = 5

def resumable_iter_content(client, stream_type, export_id):
    """Yield the export file's bytes, reconnecting on a dropped connection.

    Marketo drops the bulk-export download connection when it is held open too
    long -- e.g. while the tap is reading at the pace of a slower target. The
    export file is static and supports byte ranges, so we recover by
    re-requesting from the byte offset already yielded and continuing. Resuming
    here, below the CSV parser, keeps the byte stream contiguous so callers
    never see the seam.
    """
    start_byte = 0
    empty_resumes = 0
    while True:
        resp = client.stream_export(stream_type, export_id, start_byte=start_byte)
        bytes_this_connection = 0
        try:
            for chunk in resp.iter_content(chunk_size=CHUNK_SIZE_BYTES, decode_unicode=False):
                bytes_this_connection += len(chunk)
                start_byte += len(chunk)
                yield chunk
            return
        except (ChunkedEncodingError, ConnectionError, BrokenPipeError, ProtocolError) as ex:
            if bytes_this_connection:
                empty_resumes = 0
            else:
                empty_resumes += 1
                if empty_resumes > MAX_EMPTY_RESUMES:
                    raise ex
            singer.log_warning(
                "Export download connection dropped after %s bytes; resuming "
                "from byte %s: %s.", bytes_this_connection, start_byte, ex)
        finally:
            resp.close()


def stream_rows(client, stream_type, export_id):
    singer.log_info("Download starting.")
    chunks = resumable_iter_content(client, stream_type, export_id)
    text_stream = io.TextIOWrapper(io.BufferedReader(IterStream(chunks)), encoding='utf-8')
    try:
        reader = csv.reader(
            (line.replace('\r', '').replace('\0', '') for line in text_stream),
            delimiter=',', quotechar='"'
        )

        headers = next(reader)
        for line in reader:
            yield dict(zip(headers, line))
    finally:
        text_stream.close()
        chunks.close()


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
        fields = list(get_available_fields(stream))

        def create(export_end):
            query = {query_field: {"startAt": export_start.isoformat(),
                                   "endAt": export_end.isoformat()}}
            return client.create_export("leads", fields, query)

        # Create the new export and store the id and end date in state.
        # Does not start the export (must POST to the "enqueue" endpoint).
        export_id, export_end = create_export_with_quota_backoff(
            create,
            export_start,
            max_export_days
        )
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

        def create(export_end):
            query = {"createdAt": {"startAt": export_start.isoformat(),
                                   "endAt": export_end.isoformat()},
                     "activityTypeIds": [activity_type_id]}
            return client.create_export("activities", ACTIVITY_FIELDS, query)

        # Create the new export and store the id and end date in state.
        # Does not start the export (must POST to the "enqueue" endpoint).
        # The window is automatically shrunk and retried if a single window
        # is too large to extract within the daily API quota.
        export_id, export_end = create_export_with_quota_backoff(
            create, export_start, max_export_days)
        state = update_state_with_export_info(
            state, stream, export_id=export_id, export_end=export_end.isoformat())
    else:
        export_end = pendulum.parse(bookmarks.get_bookmark(state, stream["tap_stream_id"], "export_end"))

    return export_id, export_end


def flatten_activity(row, pan_field):
    # Start with the base fields
    rtn = {field: row[field] for field in BASE_ACTIVITY_FIELDS}

    # pan_field is the pre-computed primary attribute field name for this stream.
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


def sync_leads(client, state, stream, config):
    # http://developers.marketo.com/rest-api/bulk-extract/bulk-lead-extract/
    replication_key = determine_replication_key(stream["tap_stream_id"])

    singer.write_schema("leads", stream["schema"], stream["key_properties"], bookmark_properties=[replication_key])
    initial_bookmark = pendulum.parse(bookmarks.get_bookmark(state, "leads", replication_key))
    export_start = pendulum.parse(bookmarks.get_bookmark(state, "leads", replication_key))
    if client.use_corona:
        export_start = export_start.subtract(days=ATTRIBUTION_WINDOW_DAYS)

    job_started = utcnow()
    record_count = 0
    max_bookmark = initial_bookmark
    available_fields = get_available_fields(stream)
    while export_start < job_started:
        export_id, export_end = get_or_create_export_for_leads(client, state, stream, export_start, config)
        state = wait_for_export(client, state, stream, export_id)
        for row in stream_rows(client, "leads", export_id):
            time_extracted = utils.now()

            record = format_values(stream, row, available_fields)
            record_bookmark = pendulum.parse(record[replication_key])

            if client.use_corona:
                max_bookmark = export_end

                singer.write_record("leads", record, time_extracted=time_extracted)
                record_count += 1
            elif record_bookmark >= initial_bookmark:
                max_bookmark = max(max_bookmark, record_bookmark)

                singer.write_record("leads", record, time_extracted=time_extracted)
                record_count += 1

        # Now that one of the exports is finished, update the bookmark
        state = update_state_with_export_info(state, stream, bookmark=max_bookmark.isoformat())
        export_start = export_end

    return state, record_count


def sync_activities(client, state, stream, config):
    # http://developers.marketo.com/rest-api/bulk-extract/bulk-activity-extract/
    replication_key = determine_replication_key(stream['tap_stream_id'])
    singer.write_schema(stream["tap_stream_id"], stream["schema"], stream["key_properties"], bookmark_properties=[replication_key])
    export_start = pendulum.parse(bookmarks.get_bookmark(state, stream["tap_stream_id"], replication_key))
    job_started = utcnow()
    record_count = 0

    activity_metadata = metadata.to_map(stream["metadata"])
    pan_field = metadata.get(activity_metadata, (), 'marketo.primary-attribute-name')
    available_fields = get_available_fields(stream)

    while export_start < job_started:
        export_id, export_end = get_or_create_export_for_activities(client, state, stream, export_start, config)
        state = wait_for_export(client, state, stream, export_id)
        for row in stream_rows(client, "activities", export_id):
            time_extracted = utils.now()

            row = flatten_activity(row, pan_field)
            record = format_values(stream, row, available_fields)

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

    singer.write_schema("programs", stream["schema"], stream["key_properties"], bookmark_properties=[replication_key])
    start_date = bookmarks.get_bookmark(state, "programs", replication_key)
    end_dt = utcnow()
    end_date = end_dt.isoformat()

    if pendulum.parse(start_date) >= end_dt:
        return state, 0

    params = {
        "maxReturn": 200,
        "offset": 0,
        "earliestUpdatedAt": start_date,
        "latestUpdatedAt": end_date,
    }
    endpoint = "rest/asset/v1/programs.json"

    record_count = 0
    available_fields = get_available_fields(stream)
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
            record = format_values(stream, row, available_fields)
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
    job_started = utcnow().isoformat()
    available_fields = get_available_fields(stream)
    while True:
        data = client.request("GET", endpoint, endpoint_name=stream["tap_stream_id"], params=params)

        time_extracted = utils.now()

        # Each row just needs the values formatted. If the record is
        # newer than the original start date, stream the record. Finally,
        # update the bookmark if newer than the existing bookmark.
        for row in data["result"]:
            record = format_values(stream, row, available_fields)
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

    singer.write_schema("activity_types", stream["schema"], stream["key_properties"])
    endpoint = "rest/v1/activities/types.json"
    data = client.request("GET", endpoint, endpoint_name="activity_types")
    record_count = 0
    available_fields = get_available_fields(stream)

    time_extracted = utils.now()

    for row in data["result"]:
        record = format_values(stream, row, available_fields)
        record_count += 1

        singer.write_record("activity_types", record, time_extracted=time_extracted)

    return state, record_count


def sync(client, catalog, config, state):
    starting_stream = bookmarks.get_currently_syncing(state)
    if starting_stream:
        singer.log_info("Resuming sync from %s", starting_stream)
    else:
        singer.log_info("Starting sync")

    corona_warning_flag = False
    for stream in catalog['streams']:
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
            corona_warning_flag = True
        elif stream["tap_stream_id"].startswith("activities_"):
            state, record_count = sync_activities(client, state, stream, config)
            corona_warning_flag = True
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
    if corona_warning_flag and not client.use_corona:
            singer.log_warning(NO_CORONA_WARNING)
