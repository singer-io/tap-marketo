import csv
import io
import json
import pendulum
import singer

from singer import bookmarks

LOGGER = singer.get_logger()

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


def get_primary_field(stream):
    # The primary field is the only automatic field not in activity fields
    for field, schema in stream["schema"]["properties"].items():
        if schema["inclusion"] == "automatic" and field not in ACTIVITY_FIELDS:
            return field


def flatten_activity(stream, row):
    # Start with the base fields
    rtn = {field: row[field] for field in BASE_ACTIVITY_FIELDS}

    # Move the primary attribute to the named column
    primary_field = get_primary_field(stream)
    if primary_field:
        rtn[primary_field] = row["primaryAttributeValue"]
        rtn[primary_field + "_id"] = row["primaryAttributeValueId"]

    # Now flatten the attrs json to it's selected columns
    if "attributes" in row:
        attrs = json.loads(row["attributes"])
        for key, value in attrs.items():
            key = key.lower().replace(" ", "_")
            if stream["schema"]["properties"].get(key, {}).get("selected"):
                rtn[key] = value

    return rtn

def write_records(tap_stream_id, og_bookmark_value, lines, headers):
    if self.use_corona:
        for line in lines:
            parsed_line = parse_csv_line(line)
            singer.write_record(tap_stream_id, dict(zip(headers, parsed_line)))            

    else:
        for line in lines:
            parsed_line = parse_csv_line(line)

            if parsed_line["updatedAt"] > og_bookmark_value:
                singer.write_record(tap_stream_id, dict(zip(headers, parsed_line)))


def schedule_or_resume_export_job(state, tap_stream_id, export_id, end_date, bookmark_date, query_field, client, fields):
    if export_id is None:                
        query = {query_field: {"startAt": bookmark_date.isoformat(),
                               "endAt": end_date.isoformat()}}                        
        export_id = client.create_export("leads", fields, query)
    else:
        end_date = bookmarks.get_bookmark(state, tap_stream_id, "export_end_date")

    bookmarks.write_bookmark(state, tap_stream_id, "export_id", export_id)
    bookmarks.write_bookmark(state, tap_stream_id, "export_end_date", str(end_date))
        
    singer.write_state(state)
    return end_date, export_id
                    
def stream_leads(client, state, stream):
    use_corona = client.test_corona()

    replication_key = stream.get("replication_key")
    tap_stream_id = stream.get("tap_stream_id")
    fields = [f for f, s in stream["schema"]["properties"].items() if s.get("selected")]
    export_id = bookmarks.get_bookmark(state, tap_stream_id, "export_id")

    og_bookmark_value = pendulum.parse(bookmarks.get_bookmark(state, tap_stream_id, replication_key))
                                       
    tap_job_start_time = pendulum.utcnow()
    bookmark_date = og_bookmark_value
    if use_corona:
        query_field = "updatedAt"
    else:
        query_field = "createdAt"


    while bookmark_date < tap_job_start_time:
        end_date = bookmark_date.add(days=MAX_EXPORT_DAYS)
        if end_date > tap_job_start_time:
            end_date = tap_job_start_time

        end_date, export_id = schedule_or_resume_export_job(state, tap_stream_id, export_id, end_date, bookmark_date, query_field, client, fields) 
            
        try:
            client.wait_for_export("leads", export_id)
        except ExportFailed as ex:
            if ex.message() == "Timed out":
                ##
                LOGGER.critical("error")

            else:
                LOGGER.critical("error")
                ##fail the job
                        
        lines = client.stream_export("leads", export_id)
        headers = parse_csv_line(next(lines))

        write_records(tap_stream_id, og_bookmakr_value, lines, headers)
            
        bookmarks.write_bookmark(state, tap_stream_id, "export_id", None)
        bookmarks.write_bookmark(state, tap_stream_id, "export_end_date", None)
                                          
        if use_corona:
            bookmarks.write_bookmark(state, tap_stream_id, replication_key, end_date)
        singer.write_state(state)
        bookmark_date = end_date

    bookmarks.write_bookmark(state, tap_stream_id, replication_key, tap_job_start_time)
    singer.write_state(state)
    
        
def stream_activities(client, state, stream):
    _, activity_type_id = stream["stream"].split("_")
    export_id = state["bookmarks"][stream["stream"]].get("export_id")

    started = pendulum.utcnow()
    start_date = state["bookmarks"][stream["stream"]][stream["replication_key"]]
    start_pen = pendulum.parse(start_date)

    while start_pen < started:
        end_pen = start_pen.add(days=MAX_EXPORT_DAYS)
        if end_pen > started:
            end_pen = started

        if not export_id:
            query = {
                "createdAt": {
                    "startAt": start_pen.isoformat(),
                    "endAt": end_pen.isoformat(),
                },
                "activityTypeIds": [activity_type_id],
            }
            export_id = client.create_export("activities", ACTIVITY_FIELDS, query)
            state["bookmarks"][stream["stream"]]["export_id"] = export_id
            singer.write_state(state)

        client.wait_for_export("activities", export_id)
        lines = client.stream_export("activities", export_id)
        headers = parse_csv_line(next(lines))
        for line in lines:
            parsed_line = parse_csv_line(line)
            row = dict(zip(headers, parsed_line))
            yield flatten_activity(stream, row)

        export_id = None
        state["bookmarks"][stream["stream"]]["export_id"] = None
        singer.write_state(state)
        start_pen = end_pen


def stream_programs(client, state, stream):  # pylint: disable=unused-argument
    start_date = state["bookmarks"]["programs"]["updatedAt"]
    end_date = pendulum.utcnow().isoformat()
    params = {
        "maxReturn": 200,
        "offset": 0,
        "earliestUpdatedAt": start_date,
        "latestUpdatedAt": end_date,
    }
    endpoint = "rest/asset/v1/programs.json"

    while True:
        data = client.request("GET", endpoint, params=params)
        if NO_ASSET_MSG in data["warnings"]:
            break

        for row in data["result"]:
            yield row

        params["offset"] += params["maxReturn"]


def stream_paginated(client, state, stream):  # pylint: disable=unused-argument
    params = {"batchSize": 300}
    endpoint = "rest/v1/{}.json".format(stream["stream"])

    next_page_token = state["bookmarks"][stream["stream"]].get("next_page_token")
    if next_page_token:
        params["nextPageToken"] = next_page_token

    while True:
        data = client.request("GET", endpoint, params=params)
        for row in data["result"]:
            yield row

        if "nextPageToken" not in data:
            break

        state["bookmarks"][stream["stream"]]["next_page_token"] = data["nextPageToken"]
        singer.write_state(state)

    state["bookmarks"][stream["stream"]]["next_page_token"] = None
    singer.write_state(state)


def stream_activity_types(client, state, stream):  # pylint: disable=unused-argument
    endpoint = "rest/v1/activities/types.json"
    data = client.request("GET", endpoint)
    for row in data["result"]:
        yield row


def sync_stream(client, state, stream, stream_func):
    singer.write_schema(stream["stream"], stream["schema"], stream["key_properties"])
    start_date = state["bookmarks"][stream["stream"]]
    with singer.metrics.record_counter(stream["stream"]) as counter:
        for row in stream_func(client, state, stream):
            record = format_values(stream, row)
            if stream.get("replication_key"):
                replication_value = record[stream["replication_key"]]
                if replication_value >= start_date:
                    singer.write_record(stream["stream"], record)
                    counter.increment()
                    state["bookmarks"][stream["stream"]] = replication_value
                    singer.write_state(state)
            else:
                singer.write_record(stream["stream"], record)
                counter.increment()


def sync(client, catalog, state):
    starting_stream = state.get("current_stream")
    if starting_stream:
        LOGGER.info("Resuming sync from %s", starting_stream)
    else:
        LOGGER.info("Starting sync")

    for stream in catalog["streams"]:
        if not stream["schema"].get("selected"):
            LOGGER.info("%s: not selected", stream["stream"])
            continue

        if starting_stream and stream["stream"] != starting_stream:
            LOGGER.info("%s: already synced", stream["stream"])
            continue

        LOGGER.info("%s: starting sync", stream["stream"])
        starting_stream = None
        state["current_stream"] = stream["stream"]
        singer.write_state(state)

        if stream["stream"] == "leads":
            stream_func = stream_leads
        elif stream["stream"] == "activity_types":
            stream_func = stream_activity_types
        elif stream["stream"].startswith("activities_"):
            stream_func = stream_activities
        elif stream["stream"] in ["campaigns", "lists"]:
            stream_func = stream_paginated
        elif stream["stream"] == "programs":
            stream_func = stream_programs
        else:
            raise Exception("Not implemented")

        with singer.metrics.record_counter(stream["stream"]) as counter:
            record_count = stream_func(client, state, stream)
            counter.increment(record_count)

            
        LOGGER.info("%s: finished sync", stream["stream"])

    LOGGER.info("Finished sync")
