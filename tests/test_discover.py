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
            "replication_key": "activityDate",
            "replication_method": "INCREMENTAL",
            "metadata": [{'breadcrumb': (),
                          'metadata': {'activity_id': 1,
                                       'primary_attribute_name': 'webpage_id'}}],
            "schema": {
                "type": "object",
                "additionalProperties": False,
                "inclusion": "available",
                "properties": {
                    "marketoGUID": {
                        "type": "string",
                        "inclusion": "automatic",
                    },
                    "leadId": {
                        "type": "integer",
                        "inclusion": "automatic",
                    },
                    "activityDate": {
                        "type": "string",
                        "format": "date-time",
                        "inclusion": "automatic",
                    },
                    "activityTypeId": {
                        "type": "integer",
                        "inclusion": "automatic",
                    },
                    "primaryAttributeName": {
                        "type": "string",
                        "inclusion": "automatic",
                    },                    
                    "primaryAttributeValueId": {
                        "type": "string",
                        "inclusion": "automatic",
                    },                    
                    "primaryAttributeValue": {
                        "type": "string",
                        "inclusion": "automatic",
                    },                                        
                    "client_ip_address": {
                        "type": ["string", "null"],
                        "inclusion": "available",
                    },
                    "query_parameters": {
                        "type": ["string", "null"],
                        "inclusion": "available",
                    },
                },
            },
        }

        self.assertDictEqual(stream, get_activity_type_stream(activity))

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
            "replication_key": "updatedAt",
            "replication_method": "INCREMENTAL",
            "schema": {
                "type": "object",
                "additionalProperties": False,
                "inclusion": "available",
                "properties": {
                    "id": {
                        "type": "string",
                        "inclusion": "automatic",
                    },
                    "foo": {
                        "type": ["string", "null"],
                        "inclusion": "available",
                    },
                },
            },
        }

        with requests_mock.Mocker(real_http=True) as mock:
            mock.register_uri("GET", client.get_url("rest/v1/leads/describe.json"), json=data)
            self.assertDictEqual(stream, discover_leads(client))
