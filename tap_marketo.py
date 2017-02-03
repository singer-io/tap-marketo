#!/usr/bin/env python3

import os
import sys
import argparse
import datetime

import requests
import stitchstream as ss
import backoff
import arrow

config = None
access_token_expires = None

return_limit = 100

default_start_date = '2000-01-01T00:00:00Z'

state = {
}

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
        'Content-Type': 'www-form-urlencoded'
    }

    data = {
        'grant_type': 'client_credentials',
        'client_id': config['client_id'],
        'client_secret': config['client_secret']
    }

    response = request(url='{identity}/oauth/token'.format(config), headers=headers, data=data)

    data = response.json()

    session.headers.update('Authorization', 'Bearer {access_token}'.format(data))

    # set access_token_expires and add 10 minute buffer
    access_token_expires = datetime.datetime.now() - datetime.timedelta(seconds = data['expires_in'] - 600)

def marketo_request(**kwargs):
    global access_token_expires
    
    if access_token_expires == None or access_token_expires > datetime.datetime.now():
        refresh_token()

    return request(**kwargs)

def get_lead_batch(endpoint, lead_schema, lead_ids):
    # We're actually doing a GET request, POSTing with _method
    # allows Marketo to overcome URL / query param length limitations
    query_params = {
        '_method': 'GET'
    }

    headers = {
        'Content-Type': 'www-form-urlencoded'
    }

    data = {
        'filterType': 'id',
        'filterValues': lead_ids.join(','),
        'fields': lead_schema['properties'].keys()
    }

    response = request(url=endpoint + '/v1/leads.json',
                       params=query_params,
                       headers=headers,
                       data=data)

    data = response.json()

    ## TODO: handle errors / success == false

    ## TODO: data typing leads

    ss.write_records('leads', data['result'])

def get_leads_schema(endpoint):
    response = request(url=endpoint + '/v1/leads/describe.json')

def do_check(args):
    with open(args.config) as file:
        config = json.load(file)

    auth = (config['api_key'],'')

    params = {
    }

    try:
        request(url=base_url + '/lead/', params=params, auth=auth)
    except requests.exceptions.RequestException as e:
        logger.fatal("Error checking connection using " + e.request.url +
                     "; received status " + str(e.response.status_code) +
                     ": " + e.response.text)
        sys.exit(-1)

def get_abs_path(path):
    return os.path.join(os.path.dirname(os.path.realpath(__file__)), path)

def load_schemas(auth):
    schemas = {}

    with open(get_abs_path('tap_closeio/leads.json')) as file:
        schemas['leads'] = json.load(file)

    get_leads_schema(auth, schemas['leads'])

    with open(get_abs_path('tap_closeio/activities.json')) as file:
        schemas['activities'] = json.load(file)

    return schemas

def do_sync(args):
    global state
    with open(args.config) as file:
        config = json.load(file)

    if args.state != None:
        logger.info("Loading state from " + args.state)
        with open(args.state) as file:
            state_arg = json.load(file)
        for key in ['leads', 'activities']:
            if key in state_arg:
                state[key] = state_arg[key]

    logger.info('Replicating all Marketo data, with starting state ' + repr(state))

    session.headers.update('Authorization', 'Bearer {}'.format(config['']))

    schemas = load_schemas(auth)
    for k in schemas:
        ss.write_schema(k, schemas[k])

    try:
        get_leads(auth, schemas['leads'])
        get_activities(auth)
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
