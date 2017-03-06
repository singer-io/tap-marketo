#!/usr/bin/env python3

import collections
import datetime
import time
import sys

import requests
import singer

from tap_marketo import utils


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

logger = singer.get_logger()
session = requests.Session()


def get_start(entity):
    if entity not in STATE:
        STATE[entity] = CONFIG['start_date']

    return STATE[entity]


def refresh_token():
    url = CONFIG['identity'] + "oauth/token"
    params = {
        'grant_type': "client_credentials",
        'client_id': CONFIG['client_id'],
        'client_secret': CONFIG['client_secret'],
    }
    logger.info("Refreshing token")
    resp = requests.get(url, params=params)
    data = resp.json()

    if resp.status_code != 200 or data.get('error') == 'unauthorized':
        logger.error("Authorization failed. {}".format(data['error_description']))
        sys.exit(1)
    elif 'error' in data:
        logger.error("API returned an error. {}".format(data['error_description']))
        sys.exit(1)

    now = datetime.datetime.utcnow()
    logger.info("Token valid until {}".format(now + datetime.timedelta(seconds=data['expires_in'])))
    CONFIG['access_token'] = data['access_token']
    CONFIG['token_expires'] = now + datetime.timedelta(seconds=data['expires_in'] - 600)


@utils.ratelimit(100, 20)
def request(endpoint, params=None):
    if not CONFIG['token_expires'] or datetime.datetime.utcnow() >= CONFIG['token_expires']:
        refresh_token()

    CONFIG['call_count'] += 1
    if CONFIG['call_count'] % 250 == 0:
        check_usage()

    url = CONFIG['endpoint'] + endpoint
    params = params or {}
    headers = {'Authorization': 'Bearer {}'.format(CONFIG['access_token'])}
    req = requests.Request('GET', url, params=params, headers=headers).prepare()
    logger.info("GET {}".format(req.url))
    resp = session.send(req)
    resp.raise_for_status()
    data = resp.json()

    if not data['success']:
        reasons = ", ".join("{code}: {message}".format(**err) for err in data['errors'])
        logger.error("API call failed. {}".format(reasons))
        sys.exit(1)

    return data


def check_usage():
    data = request("v1/stats/usage.json")
    if not data.get('success'):
        raise Exception("Error occured while checking usage")

    logger.info("Used {} of {} requests".format(data['result'][0]['total'], CONFIG['max_daily_calls']))
    if data['result'][0]['total'] >= CONFIG['max_daily_calls']:
        raise Exception("Exceeded daily quota of {} requests".format(CONFIG['max_daily_calls']))


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
    elif marketo_type in ['integer', 'reference']:
        return {'type': ['null', 'integer']}
    elif marketo_type in ['float', 'currency']:
        return {'type': ['null', 'number']}
    elif marketo_type == 'boolean':
        return {'type': ['null', 'boolean']}
    return {'type': ['null', 'string']}


def get_leads_schema_and_date_fields():
    data = request("v1/leads/describe.json")['result']
    rtn = {
        "type": "object",
        "properties": {},
    }
    date_fields = []
    for row in data:
        if 'rest' not in row:
            continue

        rtn['properties'][row['rest']['name']] = datatype_to_schema(row['dataType'])
        if row['dataType'] == 'date':
            date_fields.append(row['rest']['name'])

    return rtn, date_fields


def sync_activity_types():
    activity_type_ids = set()

    for row in gen_request("v1/activities/types.json"):
        activity_type_ids.add(row['id'])
        singer.write_record("activity_types", row)

    return activity_type_ids


def sync_activities(activity_type_id):
    state_key = 'activities_{}'.format(activity_type_id)
    data = request("v1/activities/pagingtoken.json", {'sinceDatetime': get_start(state_key)})
    params = {
        'activityTypeIds': activity_type_id,
        'nextPageToken': data['nextPageToken'],
        'batchSize': 300,
    }

    lead_ids = set()
    for row in gen_request("v1/activities.json", params=params):
        lead_ids.add(row['leadId'])
        singer.write_record("activities", row)
        utils.update_state(STATE, state_key, row['activityDate'])

    return lead_ids


def sync_leads(lead_ids, fields, date_fields):
    params = {'filterType': 'id'}

    for ids in utils.chunk(sorted(lead_ids), 300):
        # We're going to have to get each batch multiple times to get all the
        # custom fields so we keep a map of id to row which can be updated
        params['filterValues'] = ','.join(map(str, ids))
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
        singer.write_records("leads", id__row.values())


def sync_lists():
    start_date = get_start("lists")
    for row in gen_request("v1/lists.json"):
        if row['updatedAt'] >= start_date:
            singer.write_record("lists", row)
            utils.update_state(STATE, "lists", row['updatedAt'])


def do_sync():
    logger.info("Starting sync")

    # Sync all activity types. We'll be using the activity type ids to
    # query for activities in the next step.
    schema = utils.load_schema("activity_types")
    singer.write_schema("activity_types", schema, ["id"])
    logger.info("Sycing activity types")
    activity_type_ids = sync_activity_types()

    # Now we sync activities one activity type at a time. While syncing
    # activities, we'll find leadIds that have been created or edited
    # that also need to be synced. Since a lead might have been altered
    # by multiple activity types, we'll collect all the leadIds into a
    # set and sync those after.
    activity_schema = utils.load_schema("activities")
    singer.write_schema("activities", activity_schema, ["id"])
    lead_ids = set()
    for activity_type_id in activity_type_ids:
        logger.info("Syncing activity type {}".format(activity_type_id))
        lead_ids.update(sync_activities(activity_type_id))

    # Now that we have the set of leadIds, we need to sync all the altered
    # leads. Once we have done that, we can update the state.
    schema, date_fields = get_leads_schema_and_date_fields()
    singer.write_schema("leads", schema, ["id"])
    logger.info("Syncing {} leads".format(len(lead_ids)))
    sync_leads(lead_ids, schema['properties'].keys(), date_fields)
    singer.write_state(STATE)

    # Finally we'll sync the contact lists and update the state.
    schema = utils.load_schema("lists")
    singer.write_schema("lists", schema, ["id"])
    logger.info("Syncing contact lists")
    sync_lists()
    singer.write_state(STATE)

    logger.info("Sync complete")


def main():
    args = utils.parse_args()

    config = utils.load_json(args.config)
    utils.check_config(config, ["endpoint", "identity", "client_id", "client_secret", "start_date"])
    CONFIG.update(config)

    if CONFIG['endpoint'][-1] != "/":
        CONFIG['endpoint'] += "/"

    if CONFIG['identity'][-1] != "/":
        CONFIG['identity'] += "/"

    if args.state:
        STATE.update(utils.load_json(args.state))

    logger.info("start_date: {}".format(CONFIG['start_date']))
    logger.info("indentity: {}".format(CONFIG['identity']))
    logger.info("endpoint: {}".format(CONFIG['endpoint']))
    logger.info("STATE: {}".format(STATE))

    do_sync()


if __name__ == '__main__':
    main()
