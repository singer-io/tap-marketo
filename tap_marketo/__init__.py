#!/usr/bin/env python3

import collections
import datetime
import os
import time
import sys

import backoff

import requests
import singer
from singer import utils
from singer.transform import transform


CONFIG = {
    "call_count": 0,
    "access_token": None,
    "token_expires": None,

    # in config file
    "endpoint": None,
    "identity": None,
    "client_id": None,
    "client_secret": None,
    "max_daily_calls": 8000,
    "start_date": None,
}
STATE = {}
LEAD_IDS = set()
LEAD_IDS_SYNCED = set()
LEADS_CHANGED_IDS = [12, 13]  # new leads and changed data values
NEW_LEADS_ID = 12
LEADS_BATCH_SIZE = 300

LOGGER = singer.get_logger()
SESSION = requests.Session()


def get_abs_path(path):
    return os.path.join(os.path.dirname(os.path.realpath(__file__)), path)

def load_schema(entity):
    return utils.load_json(get_abs_path("schemas/{}.json".format(entity)))

def get_start(entity):
    if entity not in STATE:
        STATE[entity] = CONFIG['start_date']

    return STATE[entity]

class RateLimitExceededException(Exception):
    pass

class RetryableCallFailureException(Exception):
    pass

RETRYABLE_RATE_LIMIT_ERROR_CODES = ["606", "615"]
RETRYABLE_ERROR_CODES = ["611"]

@utils.ratelimit(100, 20)
# When one of the handlers catches its associated exception, the other handler
# will be reset back to 0 tries.
@backoff.on_exception(backoff.expo,
                      (requests.exceptions.RequestException,
                       RetryableCallFailureException),
                      max_tries=5,
                      giveup=lambda e: isinstance(e, requests.exceptions.RequestException) and
                      e.response is not None and
                      400 <= e.response.status_code < 500,
                      factor=2)
@backoff.on_exception(backoff.expo,
                      RateLimitExceededException)

def request(endpoint, params=None):
    if not CONFIG['token_expires'] or datetime.datetime.utcnow() >= CONFIG['token_expires']:
        refresh_token()

    CONFIG['call_count'] += 1
    if CONFIG['call_count'] % 250 == 1:
        check_usage()

    url = CONFIG['endpoint'] + endpoint
    params = params or {}
    headers = {'Authorization': 'Bearer {}'.format(CONFIG['access_token'])}
    if 'user_agent' in CONFIG:
        headers['User-Agent'] = CONFIG['user_agent']

    req = requests.Request('GET', url, params=params, headers=headers).prepare()
    LOGGER.info("GET {}".format(req.url))
    resp = SESSION.send(req)
    if resp.status_code >= 400:
        LOGGER.critical("GET {} [{} - {}]".format(req.url, resp.status_code, resp.content))
        sys.exit(1)

    data = resp.json()

    if not data['success']:
        errors = data['errors']
        reasons = ", ".join("{code}: {message}".format(**err) for err in errors)
        if any(err['code'] in RETRYABLE_RATE_LIMIT_ERROR_CODES for err in errors):
            LOGGER.warning("Rate limit exceeded. Will try again. (Response: {})".format(reasons))
            raise RateLimitExceededException()
        else:
            retryable = any(err['code'] in RETRYABLE_ERROR_CODES for err in errors)
            exception_class = Exception
            if retryable:
                exception_class = RetryableCallFailureException
                LOGGER.warning("Retryable API call failed. {}".format(reasons))
            raise exception_class("API call failed. {}".format(reasons))

    return data


def check_usage():
    max_calls = int(CONFIG['max_daily_calls'])
    data = request("v1/stats/usage.json")
    if not data.get('success'):
        raise Exception("Error occured while checking usage")

    LOGGER.info("Used {} of {} requests".format(data['result'][0]['total'], max_calls))
    if data['result'][0]['total'] >= max_calls:
        raise Exception("Exceeded daily quota of {} requests".format(max_calls))


def refresh_token():
    url = CONFIG['identity'] + "oauth/token"
    params = {
        'grant_type': "client_credentials",
        'client_id': CONFIG['client_id'],
        'client_secret': CONFIG['client_secret'],
    }
    LOGGER.info("Refreshing token")

    try:
        resp = requests.get(url, params=params)
    except requests.exceptions.ConnectionError:
        LOGGER.critical("Connection error while refreshing token at {}. "
                        "Please check the URL matches `https://123-ABC-456.mktorest.com/identity."
                        .format(url))
        sys.exit(1)

    data = resp.json()

    if resp.status_code != 200 or data.get('error') == 'unauthorized':
        LOGGER.critical("Authorization failed. {}".format(data['error_description']))
        sys.exit(1)
    elif 'error' in data:
        LOGGER.critical("API returned an error. {}".format(data['error_description']))
        sys.exit(1)

    now = datetime.datetime.utcnow()
    LOGGER.info("Token valid until {}".format(now + datetime.timedelta(seconds=data['expires_in'])))
    CONFIG['access_token'] = data['access_token']
    CONFIG['token_expires'] = now + datetime.timedelta(seconds=data['expires_in'] - 15)


def gen_request(endpoint, params=None):
    params = params or {}
    while True:
        data = request(endpoint, params=params)
        if 'result' not in data:
            break

        for row in data['result']:
            yield row

        if data.get('moreResult', False):
            params['nextPageToken'] = data['nextPageToken']
        else:
            break


def datatype_to_schema(marketo_type):
    if marketo_type in ['datetime', 'date']:
        return {'anyOf': [{'type': 'null'}, {'type': 'string', 'format': 'date-time'}]}
    elif marketo_type in ['integer', 'percent', 'score']:
        return {'type': ['null', 'integer']}
    elif marketo_type in ['float', 'currency']:
        return {'type': ['null', 'number']}
    elif marketo_type == 'boolean':
        return {'type': ['null', 'boolean']}
    elif marketo_type in ['string', 'email', 'reference', 'url', 'phone', 'textarea',
                          'text', 'lead_function']:
        return {'type': ['null', 'string']}

    return None


def get_leads_schema_and_date_fields():
    data = request("v1/leads/describe.json")['result']

    schema = {
        "type": "object",
        "properties": {},
    }
    date_fields = []
    for row in data:
        if 'rest' not in row:
            continue

        row_type = datatype_to_schema(row['dataType'])

        if row_type is None:
            raise Exception("Unexpected dataType for leads field: {}".format(row))

        schema['properties'][row['rest']['name']] = row_type
        if row['dataType'] == 'date':
            date_fields.append(row['rest']['name'])

    return schema, date_fields


def sync_activity_types():
    activity_type_ids = set()

    for row in gen_request("v1/activities/types.json"):
        activity_type_ids.add(row['id'])
        singer.write_record("activity_types", row)

    return activity_type_ids


def sync_activities(activity_type_id, lead_fields, date_fields, leads_schema, do_leads=False):
    global LEAD_IDS, LEAD_IDS_SYNCED

    state_key = 'activities_{}'.format(activity_type_id)
    start = get_start(state_key)
    data = request("v1/activities/pagingtoken.json", {'sinceDatetime': start})
    params = {
        'activityTypeIds': activity_type_id,
        'nextPageToken': data['nextPageToken'],
        'batchSize': LEADS_BATCH_SIZE,
    }

    for row in gen_request("v1/activities.json", params=params):
        # Stream in the activity and update the state.
        singer.write_record("activities", row)
        utils.update_state(STATE, state_key, row['activityDate'])

        if do_leads:
            # Add the lead id to the set of lead ids that need synced unless
            # already synced.
            lead_id = row['leadId']
            if lead_id not in LEAD_IDS_SYNCED:
                LEAD_IDS.add(lead_id)

            # If we have 300 or more lead ids (one page), sync those leads and mark
            # the ids as synced. Once the leads have been synced we can update the
            # state.
            if len(LEAD_IDS) >= LEADS_BATCH_SIZE:
                # Take the first 300 off the set and sync them.
                lead_ids = list(LEAD_IDS)[:LEADS_BATCH_SIZE]
                sync_leads(lead_ids, lead_fields, date_fields, leads_schema)

                # Remove the synced lead ids from the set to be synced and add them
                # to the set of synced ids.
                LEAD_IDS = LEAD_IDS.difference(lead_ids)
                LEAD_IDS_SYNCED = LEAD_IDS_SYNCED.union(lead_ids)

        # Update the state.
        singer.write_state(STATE)


def sync_leads(lead_ids, fields, date_fields, leads_schema):
    LOGGER.info("Syncing {} leads".format(len(lead_ids)))
    params = {
        'filterType': 'id',
        'filterValues': ','.join(map(str, lead_ids)),
    }

    # We're going to have to get each batch multiple times to get all the
    # custom fields so we keep a map of id to row which can be updated
    id__row = collections.defaultdict(dict)

    # Chunk the fields into groups of 100 and get 300 leads with those 100
    # fields until the 300 leads are completed.
    for field_group in utils.chunk(list(fields), 100):
        params['fields'] = ','.join(field_group)
        data = request("v1/leads.json", params=params)

        for row in data['result']:
            for date_field in date_fields:
                if row.get(date_field) is not None:
                    row[date_field] += "T00:00:00Z"

            id__row[row['id']].update(row)

    # When the group of 300 leads is completely grabbed, stream them
    transformed_leads = [transform(lead, leads_schema) for lead in id__row.values()]
    singer.write_records("leads", transformed_leads)


def sync_lists():
    start_date = get_start("lists")
    for row in gen_request("v1/lists.json"):
        if row['updatedAt'] >= start_date:
            singer.write_record("lists", row)
            utils.update_state(STATE, "lists", row['updatedAt'])


def do_sync():
    global LEAD_IDS
    LOGGER.info("Starting sync")

    # First we need to send the custom leads schema in. We stream in leads
    # once we have 300 ids that have been updated.
    leads_schema, date_fields = get_leads_schema_and_date_fields()
    lead_fields = list(leads_schema['properties'].keys())
    singer.write_schema("leads", leads_schema, ["id"])

    # Sync all activity types. We'll be using the activity type ids to
    # query for activities in the next step.
    schema = load_schema("activity_types")
    singer.write_schema("activity_types", schema, ["id"])
    LOGGER.info("Sycing activity types")
    activity_type_ids = sync_activity_types()

    activity_schema = load_schema("activities")
    singer.write_schema("activities", activity_schema, ["id"])

    # Certain activity types alter leads. These activity types return lead ids
    # which have been added or altered. While syncing the activities, we track
    # the ids of these leads. When we have 300 leads (max batch size), we sync
    # the leads.
    for activity_type_id in LEADS_CHANGED_IDS:
        activity_type_ids.remove(activity_type_id)
        LOGGER.info("Syncing lead-altering activity type %d", activity_type_id)
        sync_activities(activity_type_id, lead_fields, date_fields, leads_schema, do_leads=True)

    # If there are any unsynced leads after the last mutating activity type,
    # sync them before continuing
    if LEAD_IDS:
        sync_leads(list(LEAD_IDS), lead_fields, date_fields, leads_schema)
        singer.write_state(STATE)

    # Sync the non-mutating activity types ignoring the lead ids.
    for activity_type_id in activity_type_ids:
        LOGGER.info("Syncing activity type %d", activity_type_id)
        sync_activities(activity_type_id, lead_fields, date_fields, leads_schema, do_leads=False)

    # Finally we'll sync the contact lists and update the state.
    schema = load_schema("lists")
    singer.write_schema("lists", schema, ["id"])
    LOGGER.info("Syncing contact lists")
    sync_lists()
    singer.write_state(STATE)

    LOGGER.info("Sync complete")


def main_impl():
    args = utils.parse_args(["endpoint", "identity", "client_id", "client_secret", "start_date"])
    CONFIG.update(args.config)

    if CONFIG['endpoint'][-1] != "/":
        CONFIG['endpoint'] += "/"

    if CONFIG['identity'][-1] != "/":
        CONFIG['identity'] += "/"

    if args.state:
        STATE.update(args.state)

    LOGGER.info("start_date: {}".format(CONFIG['start_date']))
    LOGGER.info("indentity: {}".format(CONFIG['identity']))
    LOGGER.info("endpoint: {}".format(CONFIG['endpoint']))

    LOGGER.info("STATE: {}".format(STATE))

    do_sync()

def main():
    try:
        main_impl()
    except Exception as exc:
        LOGGER.critical(exc)
        raise exc


if __name__ == '__main__':
    main()
