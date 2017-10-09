import csv
import io
import json
import pendulum
import singer
from singer import bookmarks

from tap_marketo.client import ExportFailed


# We can request up to 30 days worth of activities per export.
MAX_EXPORT_DAYS = 30

BASE_ACTIVITY_FIELDS = [
    "marketoGUID",
    "leadId",
    "activityDate",
    "activityTypeId",
]

ACTIVITY_FIELDS = BASE_ACTIVITY_FIELDS + [
    "primaryAttributeValue",
    "primaryAttributeValueId",
    "attributes",
]

NO_ASSET_MSG = "No assets found for the given search criteria."
NO_CORONA_WARNING = (
    "Your account does not have Corona support enabled. Without Corona, each sync of "
    "the Leads table requires a full export which can lead to lower data freshness. "
    "Please contact <contact email> at Marketo to request Corona support be added to "
    "your account."
)


def format_value(value, schema):
    if not isinstance(schema["type"], list):
        field_type = [schema["type"]]
    else:
        field_type = schema["type"]

    if value in [None, ""]:
        return None
    elif schema.get("format") == "date-time":
        return pendulum.parse(value).isoformat()
    elif "integer" in field_type:
        return int(value)
    elif "number" in field_type:
        return float(value)
    elif "boolean" in field_type:
        if isinstance(value, bool):
            return value
        return value.lower() == "true"

    return value


def format_values(stream, row):
    rtn = {}
    for field, schema in stream["schema"]["properties"].items():
        if not schema.get("selected"):
            continue
        rtn[field] = format_value(row.get(field), schema)
    return rtn


def parse_csv_line(line):
    reader = csv.reader(io.StringIO(line.decode('utf-8')))
    return next(reader)


def get_primary_field(schema):
    # The primary field is the only automatic field not in activity fields
    for field, schema in schema["properties"].items():
        if schema["inclusion"] == "automatic" and field not in ACTIVITY_FIELDS:
            return field


def flatten_activity(row, schema):
    # Start with the base fields
    rtn = {field: row[field] for field in BASE_ACTIVITY_FIELDS}

    # Move the primary attribute to the named column
    primary_field = get_primary_field(schema)
    if primary_field:
        rtn[primary_field] = row["primaryAttributeValue"]
        rtn[primary_field + "_id"] = row["primaryAttributeValueId"]

    # Now flatten the attrs json to it's selected columns
    if "attributes" in row:
        attrs = json.loads(row["attributes"])
        for key, value in attrs.items():
            key = key.lower().replace(" ", "_")
            if schema["properties"].get(key, {}).get("selected"):
                rtn[key] = value

    return rtn


def sync_activities(client, state, stream):
    # Stream names for activities are `activities_X` where X is the
    # activity type id in Marketo. We need the activity type id to
    # build the query.
    _, activity_type_id = stream["stream"].split("_")

    # Retreive stored state for the current activity stream.
    start_date = bookmarks.get_bookmark(state, stream["stream"], stream["replication_key"])
    export_id = bookmarks.get_bookmark(state, stream["stream"], "export_id")

    # Export from the existing bookmark start date until current.
    start_pen = pendulum.parse(start_date)
    job_started = pendulum.utcnow()
    record_count = 0
    while start_pen < job_started:
        if not export_id:
            # No export_id, so create a new export. Exports must have
            # a date range or Marketo returns an error.
            end_pen = start_pen.add(days=MAX_EXPORT_DAYS)
            if end_pen > job_started:
                end_pen = job_started

            # Activities must be queried by `createdAt` even though
            # that is not a real field. `createdAt` proxies `activityDate`.
            # The activity type id must also be included in the query.
            query = {
                "createdAt": {
                    "startAt": start_pen.isoformat(),
                    "endAt": end_pen.isoformat(),
                },
                "activityTypeIds": [activity_type_id],
            }

            singer.log_info("Creating %s export with query: %s", stream["stream"], query)

            # Create the export and store the id and end date in state.
            export_id = client.create_export("activities", ACTIVITY_FIELDS, query)
            state = bookmarks.write_bookmark(state,
                                             stream["stream"],
                                             "export_id",
                                             export_id)
            state = bookmarks.write_bookmark(state,
                                             stream["stream"],
                                             "export_end",
                                             end_pen.isoformat())
            singer.write_state(state)

        else:
            # If we have an export_id we are resuming an existing export.
            # Retrieve the end data of the export from state so we don't
            # re-request the same data again.
            end_date = bookmarks.get_bookmark(state, stream["stream"], "export_end")
            end_pen = pendulum.parse(end_date)
            singer.log_info("Resuming %s export %s through %s",
                            stream["stream"], export_id, end_pen)

        # Wait until export is complete. If the export fails, clear the
        # stored export id and end time before leaving.
        try:
            client.wait_for_export("activities", export_id)
        except ExportFailed:
            state = bookmarks.write_bookmark(state,
                                             stream["stream"],
                                             "export_id",
                                             None)
            state - bookmarks.write_bookmark(state,
                                             stream["stream"],
                                             "export_end",
                                             None)
            singer.write_state()
            raise

        lines = client.stream_export("activities", export_id)
        headers = parse_csv_line(next(lines))

        # Each activity must be translated from a tuple of values into
        # a map of field: value. Then the `attributes` json blob is
        # denested into fields. Finally the fields are formatted
        # according to their type.
        #
        # If the record is newer than the original bookmark (this should
        # always be the case due to our query, but just in case), we
        # update the bookmark and output the record. Finally, we updated
        # the stored bookmark is the record is newer.
        for line in lines:
            parsed_line = parse_csv_line(line)
            row = dict(zip(headers, parsed_line))
            row = flatten_activity(row, stream["schema"])
            record = format_values(stream, row)
            if record[stream["replication_key"]] >= start_date:
                record_count += 1
                singer.write_record(stream["stream"], record)
                bookmark = bookmarks.get_bookmark(state,
                                                  stream["stream"],
                                                  stream["replication_key"])
                if record[stream["replication_key"]] >= bookmark:
                    state = bookmarks.write_bookmark(state,
                                                     stream["stream"],
                                                     stream["replication_key"],
                                                     record[stream["replication_key"]])
                    singer.write_state(state)

        # After the export is complete, unset the export bookmarks,
        # advance the start date of the next query, and continue.
        export_id = None
        state = bookmarks.write_bookmark(state,
                                         stream["stream"],
                                         "export_id",
                                         None)
        state = bookmarks.write_bookmark(state,
                                         stream["stream"],
                                         "export_end",
                                         None)
        singer.write_state(state)
        start_pen = end_pen

    return state, record_count


def sync_programs(client, state, stream):
    # Programs are queryable via their updatedAt time but require and
    # end date as well. As there is no max time range for the query,
    # query from the bookmark value until current.
    # The Programs endpoint uses offsets with a return limit of 200
    # per page. If requesting past the final program, an error message
    # is returned to indicate that the endpoint has been fully synced.
    start_date = bookmarks.get_bookmark(state, "programs", "updatedAt")
    end_date = pendulum.utcnow().isoformat()
    params = {
        "maxReturn": 200,
        "offset": 0,
        "earliestUpdatedAt": start_date,
        "latestUpdatedAt": end_date,
    }
    endpoint = "rest/asset/v1/programs.json"

    # Keep querying pages of data until exhausted.
    bookmark = start_date
    record_count = 0
    while True:
        data = client.request("GET", endpoint, endpoint_name="programs", params=params)

        # If the no asset message is in the warnings, we have exhausted
        # the search results and can end the sync.
        if NO_ASSET_MSG in data["warnings"]:
            break

        # Each row just needs the values formatted. If the record is
        # newer than the original start date, stream the record. Track
        # the max updatedAt value that we see since we can't be sure
        # that the data is in chronological order.
        for row in data["result"]:
            record = format_values(stream, row)
            if record["updatedAt"] >= start_date:
                record_count += 1
                singer.write_record("programs", record)
                if record["updatedAt"] >= bookmark:
                    bookmark = record["updatedAt"]

        # Increment the offset by the return limit for the next query.
        params["offset"] += params["maxReturn"]

    # Now that we've finished every page we can update the bookmark to
    # the most recent value.
    state = bookmarks.write_bookmark(state, "programs", "updatedAt", bookmark)
    singer.write_state(state)

    return state, record_count


def sync_paginated(client, state, stream):
    # Campaigns and Static Lists are paginated with a max return of 300
    # items per page. There are no filters that can be used to only
    # return updated records.
    start_date = bookmarks.get_bookmark(state,
                                        stream["stream"],
                                        stream["replication_key"])
    params = {"batchSize": 300}
    endpoint = "rest/v1/{}.json".format(stream["stream"])

    # Paginated requests use paging tokens for retrieving the next page
    # of results. These tokens are stored in the state for resuming
    # syncs. If a paging token exists in state, use it.
    next_page_token = bookmarks.get_bookmark(state,
                                             stream["stream"],
                                             "next_page_token")
    if next_page_token:
        params["nextPageToken"] = next_page_token

    # Keep querying pages of data until no next page token.
    record_count = 0
    while True:
        data = client.request("GET", endpoint, endpoint_name=stream["stream"], params=params)

        # Each row just needs the values formatted. If the record is
        # newer than the original start date, stream the record. Finally,
        # update the bookmark if newer than the existing bookmark.
        for row in data["result"]:
            record = format_values(stream, row)
            if record[stream["replication_key"]] >= start_date:
                record_count += 1
                singer.write_record(stream["stream"], record)
                bookmark = bookmarks.get_bookmark(state,
                                                  stream["stream"],
                                                  stream["replication_key"])
                if record[stream["replication_key"]] >= bookmark:
                    state = bookmarks.write_bookmark(state,
                                                     stream["stream"],
                                                     stream["replication_key"],
                                                     record[stream["replication_key"]])
                    singer.write_state(state)

        # No next page, results are exhausted.
        if "nextPageToken" not in data:
            break

        # Store the next page token in state and continue.
        state = bookmarks.write_bookmark(state,
                                         stream["stream"],
                                         "next_page_token",
                                         data["nextPageToken"])
        singer.write_state(state)

    # Once all results are exhausted, unset the next page token bookmark
    # so the subsequent sync starts from the beginning.
    state = bookmarks.write_bookmark(state,
                                     stream["stream"],
                                     "next_page_token",
                                     None)
    singer.write_state(state)
    return state, record_count


def sync_activity_types(client, state, stream):
    # Activity types aren't even paginated. Grab all the results in one
    # request, format the values, and output them.
    endpoint = "rest/v1/activities/types.json"
    data = client.request("GET", endpoint, endpoint_name="activity_types")
    record_count = 0
    for row in data["result"]:
        record = format_values(stream, row)
        record_count += 1
        singer.write_record("activity_types", record)

    return state, record_count


def sync(client, catalog, state):
    starting_stream = bookmarks.get_currently_syncing(state)
    if starting_stream:
        singer.log_info("Resuming sync from %s", starting_stream)
    else:
        singer.log_info("Starting sync")

    for stream in catalog["streams"]:
        # Skip unselected streams.
        if not stream["schema"].get("selected"):
            singer.log_info("%s: not selected", stream["stream"])
            continue

        # Skip streams that have already be synced when resuming.
        if starting_stream and stream["stream"] != starting_stream:
            singer.log_info("%s: already synced", stream["stream"])
            continue

        singer.log_info("%s: starting sync", stream["stream"])

        # Now that we've started, there's no more "starting stream". Set
        # the current stream to resume on next run.
        starting_stream = None
        state = bookmarks.set_currently_syncing(state, stream["stream"])
        singer.write_state(state)

        # Sync stream based on type.
        if stream["stream"] == "activity_types":
            state, record_count = sync_activity_types(client, state, stream)
        elif stream["stream"].startswith("activities_"):
            state, record_count = sync_activities(client, state, stream)
        elif stream["stream"] in ["campaigns", "lists"]:
            state, record_count = sync_paginated(client, state, stream)
        elif stream["stream"] == "programs":
            state, record_count = sync_programs(client, state, stream)
        else:
            raise Exception("Stream %s not implemented" % stream["stream"])

        # Emit metric for record count.
        counter = singer.metrics.record_counter(stream["stream"])
        counter.value = record_count
        counter._pop()

        # Unset current stream.
        state = bookmarks.set_currently_syncing(state, None)
        singer.write_state(state)
        singer.log_info("%s: finished sync", stream["stream"])

    singer.log_info("Finished sync")

    # If Corona is not supported, log a warning near the end of the tap
    # log with instructions on how to get Corona supported.
    if not client.use_corona:
        singer.log_warning(NO_CORONA_WARNING)
