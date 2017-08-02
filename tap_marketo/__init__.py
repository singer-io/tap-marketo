#!/usr/bin/env python3

import backoff
import datetime
import json
import os
import sys

import requests
import singer
from singer import (
    bookmarks,
    metrics,
    Transformer,
    utils,
)


REQUIRED_CONFIG_KEYS = ["endpoint", "identity", "client_id", "client_secret", "start_date"]
LEADS_CHANGED_IDS = [12, 13]  # new leads and changed data values
LEADS_BATCH_SIZE = 300
DEFAULT_MAX_DAILY_CALLS = 8000

LOGGER = singer.get_logger()


def get_abs_path(path):
    return os.path.join(os.path.dirname(os.path.realpath(__file__)), path)


def load_schema(entity):
    return utils.load_json(get_abs_path("schemas/{}.json".format(entity)))


class SyncErrorsDetected(Exception):
    def __init__(self, errors):
        self.errors = errors
        errs = ["{}: {}".format(name, exc) for name, exc in errors]
        msg = "Errors occured during sync:\n\t{}".format("\n\t".join(errs))
        super(SyncErrorsDetected, self).__init__(msg)


class Client:
    def __init__(self, endpoint, identity, client_id, client_secret, start_date,
                 max_daily_calls=DEFAULT_MAX_DAILY_CALLS,
                 user_agent=None):
        self.endpoint = endpoint
        if not self.endpoint[-1] == "/":
            self.endpoint += "/"

        self.identity = identity
        if not self.identity[-1] == "/":
            self.identity += "/"

        self.client_id = client_id
        self.client_secret = client_secret
        self.start_date = start_date
        self.max_daily_calls = int(max_daily_calls)
        self.user_agent = user_agent

        self._access_token = None
        self._token_expires = None
        self._session = requests.Session()

        self.refresh_token()
        self.update_call_count()

    @classmethod
    def from_config(cls, config):
        return cls(**config)

    def get_url(self, endpoint):
        return self.endpoint + endpoint

    def get_headers(self):
        rtn = {"Authorization": "Bearer {}".format(self._access_token)}
        if self.user_agent:
            rtn['User-Agent'] = self.user_agent
        return rtn

    def refresh_token(self):
        LOGGER.info("Refreshing token...")
        url = self.identity + "oauth/token"
        params = {
            "grant_type": "client_credentials",
            "client_id": self.client_id,
            "client_secret": self.client_secret,
        }

        try:
            resp = self._session.get(url, params=params)
            data = resp.json()
        except requests.exceptions.ConnectionError:
            LOGGER.error("Connection error while refreshing token at {}." .format(url))
            raise

        if resp.status_code != 200:
            if data.get("error") == "unauthorized":
                raise Exception("Authorization failed. {}".format(data['error_description']))
            else:
                raise Exception("API returned an error. {}".format(data['error_description']))

        now = datetime.datetime.utcnow()
        LOGGER.info("Token valid until %s", now + datetime.timedelta(seconds=data['expires_in']))
        self._access_token = data['access_token']
        self._token_expires = now + datetime.timedelta(seconds=data['expires_in'] - 15)

    def update_call_count(self):
        LOGGER.info("Checking remaining API call count...")
        url = self.get_url("v1/stats/usage.json")
        headers = self.get_headers()
        resp = self._session.get(url, headers=headers)
        data = resp.json()
        if not data.get('success'):
            raise Exception("Error occured checking usage.")

        self._call_count = data['result'][0]['total']
        LOGGER.info("Used %s of %s requests.", self._call_count, self.max_daily_calls)

    @utils.ratelimit(100, 20)
    @backoff.on_exception(backoff.expo,
                          (requests.exceptions.RequestException),
                          max_tries=5,
                          giveup=lambda e: e.response is not None and 400 <= e.response.status_code < 500,
                          factor=2)
    def request(self, method, endpoint, **kwargs):
        if self._call_count >= self.max_daily_calls:
            raise Exception("Exceeded daily quota of {} requests. Ending sync.".format(self.max_daily_calls))

        if not self._access_token or datetime.datetime.utcnow() >= self._token_expires:
            self.refresh_token()

        self._call_count += 1
        if self._call_count % 250 == 0:
            self.update_call_count()

        url = self.get_url(endpoint)
        headers = self.get_headers()
        req = requests.Request(method, url, headers=headers, **kwargs).prepare()
        LOGGER.info("%s %s", method, req.url)

        with metrics.http_request_timer(endpoint=endpoint):
            resp = self._session.send(req)

        if resp.status_code >= 400:
            LOGGER.error("%s %s [%s %s]", method, req.url, resp.status_code, resp.content)

        resp.raise_for_status()

        data = resp.json()
        if not data['success']:
            reasons = ",".join("{code}: {message}".format(**err) for err in data['errors'])
            raise Exception("API call failed. {}".format(reasons))

        return data

    def gen_request(self, method, endpoint, **kwargs):
        params = kwargs.pop("params", {})
        while True:
            data = self.request(method, endpoint, params=params, **kwargs)
            if "result" not in data:
                break

            for row in data["result"]:
                yield row

            if data.get("moreResult"):
                params["nextPageToken"] = data["nextPageToken"]
            else:
                break


class ActivityTypeStream:
    def __init__(self, client, state):
        self._client = client
        self._state = state

    @property
    def schema(self):
        if not hasattr(self, "_schema"):
            setattr(self, "_schema", load_schema("activity_types"))
        return self._schema

    def sync(self):
        activity_type_ids = set()
        with metrics.record_counter(endpoint="activity_types") as counter:
            with Transformer() as transformer:
                for row in self._client.gen_request("GET", "v1/activities/types.json"):
                    transformed = transformer.transform(row, self.schema)
                    singer.write_record("activity_types", transformed)
                    activity_type_ids.add(transformed["id"])
                    counter.increment()

        return activity_type_ids


class LeadsStream:
    def __init__(self, client, state):
        self._client = client
        self._state = state
        self._to_sync = set()
        self._synced = set()
        self._counter = metrics.Counter(metrics.Metric.record_count, {metrics.Tag.endpoint: "leads"})
        self._transformer = Transformer()

    @staticmethod
    def datatype_to_schema(datatype):
        if datatype in ['datetime', 'date']:
            return {'anyOf': [{'type': 'null'}, {'type': 'string', 'format': 'date-time'}]}
        elif datatype in ['integer', 'percent', 'score']:
            return {'type': ['null', 'integer']}
        elif datatype in ['float', 'currency']:
            return {'type': ['null', 'number']}
        elif datatype == 'boolean':
            return {'type': ['null', 'boolean']}
        elif datatype in ['string', 'email', 'reference', 'url', 'phone', 'textarea', 'text', 'lead_function']:
            return {'type': ['null', 'string']}
        else:
            raise Exception("Unexpected dataType for leads field: {}".format(row))

    def _get_schema(self):
        data = self._client.request("GET", "v1/leads/describe.json")["result"]
        schema = {
            "type": "object",
            "properties": {},
        }
        for row in data:
            if "rest" not in row:
                continue

            row_type = self.datatype_to_schema(row["dataType"])
            schema["properties"][row["rest"]["name"]] = row_type

        return schema

    @property
    def schema(self):
        if not hasattr(self, "_schema"):
            setattr(self, "_schema", self._get_schema())
        return self._schema

    @property
    def fields(self):
        return list(self.schema["properties"].keys())

    def sync(self, lead_ids):
        params = {
            "filterType": "id",
            "filterValues": ",".join(map(str, lead_ids)),
        }

        for field_group in utils.chunk(self.fields, 100):
            params["fields"] = ",".join(field_group)
            data = self._client.request("GET", "v1/leads.json", params=params)

            for row in data["result"]:
                transformed = self._transformer.transform(row, self.schema)
                singer.write_record("leads", transformed)
                self._counter.increment()

        self._to_sync = self._to_sync.difference(lead_ids)
        self._synced = self._synced.union(lead_ids)

    def add(self, lead_id):
        if lead_id not in self._to_sync and lead_id not in self._synced:
            self._to_sync.add(lead_id)

        if len(self._to_sync) >= LEADS_BATCH_SIZE:
            self.sync(list(self._to_sync))

    def finish(self):
        if len(self._to_sync):
            self.sync(list(self._to_sync))

        self._counter._pop()
        self._transformer.log_warning()


class ActivityStream:
    def __init__(self, client, state, type_id, mutates=False):
        self._client = client
        self._state = state
        self.type_id = type_id
        self.mutates = mutates

    @property
    def schema(self):
        if not hasattr(self, "_schema"):
            setattr(self, "_schema", load_schema("activities"))
        return self._schema

    @property
    def state_key(self):
        return "activities_{}".format(self.type_id)

    def sync(self, leads_stream):
        start = bookmarks.get_bookmark(self._state, self.state_key, "sinceDatetime") or self._client.start_date
        data = self._client.request("GET", "v1/activities/pagingtoken.json", params={"sinceDatetime": start})
        params = {
            "activityTypeIds": self.type_id,
            "nextPageToken": data["nextPageToken"],
            "batchSize": LEADS_BATCH_SIZE,
        }

        with metrics.record_counter(endpoint=self.state_key) as counter:
            with Transformer() as transformer:
                for row in self._client.gen_request("GET", "v1/activities.json", params=params):
                    transformed = transformer.transform(row, self.schema)
                    singer.write_record("activities", transformed)
                    counter.increment()
                    bookmarks.write_bookmark(self._state, self.state_key, "sinceDatetime", row["activityDate"])
                    singer.write_state(self._state)

                    if self.mutates:
                        leads_stream.add(row["leadId"])

        if self.mutates:
            leads_stream.finish()


class ListsStream:
    def __init__(self, client, state):
        self._client = client
        self._state = state

    @property
    def schema(self):
        if not hasattr(self, "_schema"):
            setattr(self, "_schema", load_schema("lists"))
        return self._schema

    def sync(self):
        start = bookmarks.get_bookmark(self._state, "lists", "updatedAt") or self._client.start_date
        with metrics.record_counter(endpoint="lists") as counter:
            with Transformer() as transformer:
                for row in self._client.gen_request("GET", "v1/lists.json"):
                    transformed = transformer.transform(row, self.schema)
                    if transformed["updatedAt"] < start:
                        continue

                    singer.write_record("lists", transformed)
                    counter.increment()
                    bookmarks.write_bookmark(self._state, "lists", "updatedAt", transformed["updatedAt"])
                    singer.write_state(self._state)


def do_sync(client, state):
    LOGGER.info("Sync starting")
    errors = []

    leads_stream = LeadsStream(client, state)
    singer.write_schema("leads", leads_stream.schema, ["id"])

    activity_type_stream = ActivityTypeStream(client, state)
    singer.write_schema("activity_types", activity_type_stream.schema, ["id"])

    try:
        activity_type_ids = activity_type_stream.sync()
    except Exception as exc:
        LOGGER.exception("An error occured while syncing activity_types")
        errors.append(("activity_types", exc))

    #write the activity schema only once
    dummy_activity_stream = ActivityStream(client, state, 0)
    singer.write_schema("activities", dummy_activity_stream.schema, ["id"])

    # We need to reorder the list of activity type ids so that the mutating ids
    # are first.
    for activity_type_id in LEADS_CHANGED_IDS:
        activity_type_ids.remove(activity_type_id)
        activity_type_ids = [activity_type_id] + list(activity_type_ids)

    # Now we sync activity types with the leads-mutating activity types done
    # first. If a job finished midway, the currently_syncing will be set and we
    # can skip activities that do not match
    for activity_type_id in activity_type_ids:
        currently_syncing = bookmarks.get_currently_syncing(state)
        if currently_syncing and activity_type_id != currently_syncing:
            LOGGER.info("Skipping %d", activity_type_id)
            continue

        activity_stream = ActivityStream(client, state, activity_type_id, activity_type_id in LEADS_CHANGED_IDS)
        bookmarks.set_currently_syncing(state, activity_stream.state_key)
        try:
            activity_stream.sync(leads_stream)
        except Exception as exc:
            LOGGER.exception("An error occured while syncing %s", activity_stream.state_key)
            raise
            errors.append((activity_stream.state_key, exc))

        bookmarks.set_currently_syncing(state, None)

    # Now we sync lists
    lists_stream = ListsStream(client, state)
    singer.write_schema("lists", lists_stream.schema, ["id"])
    try:
        lists_stream.sync()
    except Exception as exc:
        LOGGER.exception("An error occured while syncing lists")
        errors.append(("lists", exc))

    # If errors were detected during sync, don't exit 0 and let the user know
    if errors:
        raise SyncErrorsDetected(errors)
    else:
        LOGGER.info("Sync completed successfully")


def do_discover(client, state):
    LOGGER.info("Discovery starting")

    schemas = []

    leads_stream = LeadsStream(client, state)
    schemas.append({
        "stream": "leads",
        "tap_stream_id": "leads",
        "schema": leads_stream.schema,
    })

    activity_type_stream = ActivityTypeStream()
    schemas.append({
        "stream": "activity_types",
        "tap_stream_id": "activity_types",
        "schema": activity_type_stream.schema,
    })

    activity_stream = ActivityStream(0)
    schemas.append({
        "stream": "activities",
        "tap_stream_id": "activities",
        "schema": activity_stream.schema,
    })

    lists_stream = ListsStream()
    schemas.append({
        "stream": "lists",
        "tap_stream_id": "lists",
        "schema": lists_stream.schema,
    })

    json.dump({"streams": streams}, sys.stdout, indent=4)
    LOGGER.info("Finished discovery")


def convert_legacy_state_if_needed(state):
    if "bookmarks" in state:
        return state

    new_state = {"bookmarks": {}, "currently_syncing": None}
    for tap_stream_id, start_date in state.items():
        new_state["bookmarks"][tap_stream_id] = {"sinceDatetime": start_date}

    return new_state


def main():
    args = singer.parse_args(REQUIRED_CONFIG_KEYS)
    client = Client.from_config(args.config)
    state = convert_legacy_state_if_needed(args.state)

    LOGGER.info("start_date: %s", client.start_date)
    LOGGER.info("identity: %s", client.identity)
    LOGGER.info("endpoint: %s", client.endpoint)
    LOGGER.info("STATE: %s", state)

    if args.discover:
        do_discover(client, state)
    else:
        do_sync(client, state)


if __name__ == '__main__':
    main()
