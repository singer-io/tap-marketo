import csv
import io
import json

import pendulum
import singer


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

class Stream(object):
    def __init__(self, tap_stream_id, pk_fields):
        self.tap_stream_id=tap_stream_id
        self.pk_fields = pk_fields
        
class Leads(Stream):
    def __init__(self, **kwargs):
        super().__init__(self, **kwargs)
        self.use_corona = ctx.client.test_corona
        
        self.fields = [f for f, s in stream["schema"]["properties"].items() if s.get("selected")]
        self.export_id = state.get("leads_export_id")

        self.og_bookmark_value = pendulum.parse(state["bookmarks"]["leads"])
        self.attempts = 0
        self.tap_job_start_time = pendulum.utcnow()
        self.bookmark_date = self.og_bookmark_value
        if self.use_corona:
            self.query_field = "updatedAt"
        else:
            self.query_field = "createdAt"

    def schedule_or_resume_export_job(self, end_date):
        if not self.export_id:                
            query = {query_field: {"startAt": self.bookmark_date.isoformat(),
                                   "endAt": end_date.isoformat()}}
                        
            self.export_id = client.create_export("leads", fields, query)
                
            state["leads_export_id"] = self.export_id
            singer.write_state(state)

    def calculate_end_date(self, days_to_add=MAX_EXPORT_DAYS):
        end_date = self.bookmark_date.add(days=days_to_add)
        if end_date > self.tap_job_start_time:
            end_date = job_start_time
            
    def sync(self, ctx):
        while self.bookmark_date < self.tap_job_start_time:
            if self.attempts > 4:
                #fail the job
            
            end_date = self.calculate_end_date(MAX_EXPORT_DAYS)
            if end_date > self.tap_job_start_time:
                end_date = job_start_time
                
            self.schedule_or_resume_export_job(self, end_date)

            try:
                client.wait_for_export("leads", self.export_id)
            except ExportFailed as ex:
                if ex.message() == "Timed out":
                    #halve query window and try again

                    self.attempts += 1
                    end_date = calculate_end_date(self, 15)
                    self.export_id = None
                    self.schedule_or_resume_export_job(end_date)
                else:
                    #fail the job
                        
            lines = client.stream_export("leads", self.export_id)
            headers = parse_csv_line(next(lines))

            self.write_records(lines, headers)
            
            state["leads_export_id"] = None

            if self.use_corona:
                state["bookmarks"]["leads"] = end_date
            singer.write_state(state)
            self.bookmark_date = end_date

        state["bookmarks"]["leads"] = self.tap_job_start_time
        singer.write_state(state)


    def write_records(self, lines, headers):
        if self.use_corona:
            for line in lines:
                parsed_line = parse_csv_line(line)
                yield dict(zip(headers, parsed_line)) #fix
        else:
            for line in lines:
                parsed_line = parse_csv_line(line)

                ##fix                
                if parsed_line["updatedAt"] > self.og_bookmark_value:
                    singer.write_record(self.tap_stream_id, dict(zip(headers, parsed_line)))
        
def stream_leads(client, state, stream):
    
    fields = [f for f, s in stream["schema"]["properties"].items() if s.get("selected")]
    export_id = state.get("leads_export_id")

    tap_job_start_time = pendulum.utcnow()
    bookmark_date = state["bookmarks"]["leads"]

    bookmark_date = pendulum.parse(bookmark_date)

    while bookmark_date < tap_job_start_time:
        end_date = bookmark_date.add(days=MAX_EXPORT_DAYS)
        if end_date > tap_job_start_time:
            end_date = job_start_time

        if not export_id:
            if client.use_corona:
                query_field = "updatedAt"
            else:
                query_field = "createdAt"

            query = {query_field: {"startAt": bookmark_date.isoformat(),
                                   "endAt": end_date.isoformat()}}

            export_id = client.create_export("leads", fields, query)

            state["leads_export_id"] = export_id
            singer.write_state(state)

        ## full-table from Marketo (heavier use of export quota but still
        ## within limits if they don't run more than once a day) but
        ## filtered in-memory to reduce row count
        ## handle failure case here.  Shrink export window?
        client.wait_for_export("leads", export_id)
        lines = client.stream_export("leads", export_id)
        headers = parse_csv_line(next(lines))
        for line in lines:
            parsed_line = parse_csv_line(line)
            yield dict(zip(headers, parsed_line))

        state["leads_export_id"] = None
        if client.use_corona:
          state["bookmarks"]["leads"] = end_date
        singer.write_state(state)
        bookmark_date = end_date
        
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

        sync_stream(client, state, stream, stream_func)
        LOGGER.info("%s: finished sync", stream["stream"])

    LOGGER.info("Finished sync")


streams_as_objects = [
    Leads("leads", ["updatedAt"])
]
