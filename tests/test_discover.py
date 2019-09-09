import unittest

import pendulum
import requests_mock

from tap_marketo.client import Client
from tap_marketo.discover import *


class TestDiscover(unittest.TestCase):
    def test_get_activity_type_stream(self):
        activity = {
            "id": 1,
            "name": "Visit Webpage",
            "description": "User visits a web page",
            "primaryAttribute": {
                "name": "Webpage ID",
                "dataType": "integer",
            },
            "attributes": [
                {
                    "name": "Client IP Address",
                    "dataType": "string",
                },
                {
                    "name": "Query Parameters",
                    "dataType": "string",
                },
            ],
        }

        stream = {
            "tap_stream_id": "activities_visit_webpage",
            "stream": "activities_visit_webpage",
            "key_properties": ["marketoGUID"],
            "metadata" : [
                {'breadcrumb': (),
                 'metadata': {'marketo.activity-id': 1,
                              'marketo.primary-attribute-name': 'webpage_id'}},
                {
                    "metadata" : {
                        "inclusion": "automatic"
                    },
                    "breadcrumb" : ("properties", 'marketoGUID')
                },
                {
                    "metadata" : {
                        "inclusion": "automatic"
                    },
                    "breadcrumb" : ("properties", 'leadId')
                },
                {
                    "metadata" : {
                        "inclusion": "automatic"
                    },
                    "breadcrumb" : ("properties", 'activityDate')
                },
                {
                    "metadata" : {
                        "inclusion": "automatic"
                    },
                    "breadcrumb" : ("properties", 'activityTypeId')
                },
                {
                    "metadata" : {
                        "inclusion": "automatic"
                    },
                    "breadcrumb" : ("properties", 'primary_attribute_name')
                },
                {
                    "metadata" : {
                        "inclusion": "automatic"
                    },
                    "breadcrumb" : ("properties", 'primary_attribute_value_id')
                },
                {
                    "metadata" : {
                        "inclusion": "automatic"
                    },
                    "breadcrumb" : ("properties", 'primary_attribute_value')
                },
                {
                    "metadata" : {
                        "inclusion": "available"
                    },
                    "breadcrumb" : ("properties", 'client_ip_address')
                },
                {
                    "metadata" : {
                        "inclusion": "available"
                    },
                    "breadcrumb" : ("properties", 'query_parameters')
                },
            ],
            "schema": {
                "type": "object",
                "additionalProperties": False,
                "properties": {
                    "marketoGUID": {
                        "type": ["null", "string"],
                    },
                    "leadId": {
                        "type": ["null", "integer"],
                    },
                    "activityDate": {
                        "type": ["null", "string"],
                        "format": "date-time",
                    },
                    "activityTypeId": {
                        "type": ["null", "integer"],
                    },
                    "primary_attribute_name": {
                        "type": ["null", "string"],
                    },
                    "primary_attribute_value_id": {
                        "type": ["null", "string"],
                    },
                    "primary_attribute_value": {
                        "type": ["null", "string"],
                    },
                    "client_ip_address": {
                        "type": ["string", "null"],
                    },
                    "query_parameters": {
                        "type": ["string", "null"],
                    },
                },
            },
        }
        result = get_activity_type_stream(activity)
        result_metadata = result.pop("metadata")
        stream_metadata = stream.pop('metadata')
        automatic_count = 0
        for mdata in result_metadata:
            if mdata['metadata'].get('inclusion') == 'automatic':
                automatic_count += 1
        self.assertDictEqual(stream, result)
        self.assertEqual(sorted(result_metadata, key=lambda x: x['breadcrumb']),
                         sorted(stream_metadata, key=lambda x: x['breadcrumb']))
        self.assertEqual(10, len(result_metadata))
        self.assertEqual(7,automatic_count)

    def test_discover_leads(self):
        client = Client("123-ABC-456", "id", "secret")
        client.token_expires = pendulum.utcnow().add(days=1)
        client.calls_today = 1
        data = {
            "success": True,
            "result": [
                {"displayName": "id", "dataType": "string", "rest": {"name": "id"}},
                {"displayName": "foo", "dataType": "string", "rest": {"name": "foo"}},
                {"displayName": "bar", "dataType": "string", "soap": {"name": "bar"}},
            ],
        }

        stream = {
            "tap_stream_id": "leads",
            "stream": "leads",
            "key_properties": ["id"],
            "schema": {
                "type": "object",
                "additionalProperties": False,
                "properties": {
                    "id": {
                        "type": "string",
                    },
                    "foo": {
                        "type": ["string", "null"],
                    },
                },
            },
        }

        with requests_mock.Mocker(real_http=True) as mock:
            mock.register_uri("GET", client.get_url("rest/v1/leads/describe.json"), json=data)
            self.maxDiff = None
            result = discover_leads(client)
            metadata = result.pop("metadata")
            automatic_count = 0
            for mdata in metadata:
                if mdata['metadata']['inclusion'] == 'automatic':
                    automatic_count += 1
            self.assertDictEqual(stream, result)
            self.assertEqual(2,len(metadata))
            self.assertEqual(1,automatic_count)
