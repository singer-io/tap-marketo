#!/usr/bin/env python3

import os
import sys
import argparse
import datetime
import json

import requests
import stitchstream as ss
import backoff

config = None
access_token_expires = None

return_limit = 100

default_start_date = '2000-01-01T00:00:00Z'

schemas = {}
state = {
    'new_leads': default_start_date
}
lead_activity_types = None

logger = ss.get_logger()

session = requests.Session()

class StitchException(Exception):
    """Used to mark Exceptions that originate within this tap."""
    def __init__(self, message):
        self.message = message

def client_error(e):
    return e.response is not None and 400 <= e.response.status_code < 500

@backoff.on_exception(backoff.expo,
                      (requests.exceptions.RequestException),
                      max_tries=5,
                      giveup=client_error,
                      factor=2)
def request(**kwargs):
    if 'method' not in kwargs:
        kwargs['method'] = 'get'

    response = session.request(**kwargs)
    response.raise_for_status()
    return response

def refresh_token():
    global config, access_token_expires
    
    headers = {
        'Content-Type': 'application/x-www-form-urlencoded'
    }

    data = {
        'grant_type': 'client_credentials',
        'client_id': config['client_id'],
        'client_secret': config['client_secret']
    }

    response = request(method='post', 
                       url='{identity}/oauth/token'.format(**config),
                       headers=headers,
                       data=data)

    data = response.json()

    session.headers.update({'Authorization': 'Bearer {access_token}'.format(**data)})

    # set access_token_expires and add 10 minute buffer
    access_token_expires = datetime.datetime.now() - datetime.timedelta(seconds = data['expires_in'] - 600)

def marketo_request(path, **kwargs):
    kwargs['url'] = config['endpoint'] + path
    
    if access_token_expires == None or access_token_expires > datetime.datetime.now():
        refresh_token()

    response = request(**kwargs)

    body = response.json()

    if 'success' in body and body['success'] == False:
        raise StitchException('Unsuccessful request to ' + kwargs['url'] + ' ' + response.text)

    return body

def marketo_request_paging(path, f=None, **kwargs):
    body = marketo_request(path, **kwargs)
    
    if f != None:
        f(body['result'])

    if 'moreResult' in body and body['moreResult'] == True:
        if 'params' not in kwargs:
            kwargs['params'] = {}
        kwargs['params']['nextPageToken'] = body['nextPageToken']

        return body['result'] + marketo_request_paging(path, f, **kwargs)

    return body['result']

def get_activity_types():
    global lead_activity_types

    data = marketo_request_paging('/v1/activities/types.json')

    lead_activity_types = data

    ss.write_records('lead_activity_types', data)

def get_lead_batch(lead_ids):
    # We're actually doing a GET request, POSTing with _method
    # allows Marketo to overcome URL / query param length limitations
    query_params = {
        '_method': 'GET'
    }

    headers = {
        'Content-Type': 'application/x-www-form-urlencoded'
    }

    data = {
        'filterType': 'id',
        'filterValues': ','.join(lead_ids),
        'fields': ','.join(list(schemas['leads']['properties'].keys()))
    }

    data = marketo_request('/v1/leads.json',
                           method='post',
                           params=query_params,
                           headers=headers,
                           data=data)

    ## TODO: data typing leads

    ss.write_records('leads', data['result'])

def get_new_lead_activity():
    params = {'sinceDatetime': state['new_leads']}
    paging_token = marketo_request('/v1/activities/pagingtoken.json', params=params)['nextPageToken']

    for activity_type in lead_activity_types:
        if activity_type['name'] == 'New Lead':
            new_lead_activity_id = activity_type['id']
            break

    params = {
        'activityTypeIds': new_lead_activity_id,
        'nextPageToken': paging_token,
        'batchSize': 300
    }

    def persist(lead_activities):
        ss.write_records('lead_activities', lead_activities)
        get_lead_batch(list(map(lambda x: str(x['leadId']), lead_activities)))

    marketo_request_paging('/v1/activities.json', params=params, f=persist)

def marketo_to_json_type(marketo_type):
    if marketo_type in ['datetime', 'date']:
        return {
            'type': ['null','string'],
            'format': 'date-time'
        }
    if marketo_type == 'integer':
        return {'type': ['null','integer']}
    if marketo_type in ['float','currency']:
        return {'type': ['null','number']}
    if marketo_type == 'boolean':
        return {'type': ['null','boolean']}
    return {'type': ['null','string']}

def get_leads_schema():
    data = marketo_request_paging('/v1/leads/describe.json')

    properties = {}
    for field in data:
        data_type = marketo_to_json_type(field['dataType'])
        field_name = field['rest']['name']
        if field_name == 'id':
            data_type['key'] = True
        properties[field_name] = data_type

    return {
        'type': 'object',
        'properties': properties
    }

def do_check(args):
    global config

    with open(args.config) as file:
        config = json.load(file)

    try:
        marketo_request('/v1/leads/describe.json')
    except requests.exceptions.RequestException as e:
        logger.fatal("Error checking connection using " + e.request.url +
                     "; received status " + str(e.response.status_code) +
                     ": " + e.response.text)
        sys.exit(-1)

def get_abs_path(path):
    return os.path.join(os.path.dirname(os.path.realpath(__file__)), path)

def load_schemas():
    schemas = {}

    schemas['leads'] = get_leads_schema()

    with open(get_abs_path('tap_marketo/lead_activity_types.json')) as file:
        schemas['lead_activity_types'] = json.load(file)

    with open(get_abs_path('tap_marketo/lead_activities.json')) as file:
        schemas['lead_activities'] = json.load(file)

    return schemas

def do_sync(args):
    global config, schemas, state

    with open(args.config) as file:
        config = json.load(file)

    if args.state != None:
        logger.info("Loading state from " + args.state)
        with open(args.state) as file:
            state_arg = json.load(file)
        for key in ['leads', 'new_leads', 'lead_activities']:
            if key in state_arg:
                state[key] = state_arg[key]

    logger.info('Replicating all Marketo data, with starting state ' + repr(state))

    ## TODO: check usage

    schemas = load_schemas()
    for k in schemas:
        ss.write_schema(k, schemas[k])

    try:
        get_activity_types()
        get_new_lead_activity()
        logger.info("Tap exiting normally")
    except requests.exceptions.RequestException as e:
        logger.fatal("Error on " + e.request.url +
                     "; received status " + str(e.response.status_code) +
                     ": " + e.response.text)
        sys.exit(-1)

def main():
    global logger
    parser = argparse.ArgumentParser()

    parser.add_argument(
        '-c', '--config', help='Config file', required=True)
    parser.add_argument(
        '-s', '--state', help='State file')

    args = parser.parse_args()

    do_sync(args)

if __name__ == '__main__':
    main()
