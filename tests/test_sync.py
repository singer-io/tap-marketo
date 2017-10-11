import unittest
import unittest.mock
import urllib.parse

import freezegun
import pendulum
import requests_mock

from tap_marketo.client import Client, ApiException
from tap_marketo.discover import discover_catalog
from tap_marketo.sync import *


def parse_params(request):
    return dict(urllib.parse.parse_qsl(request.query))


class TestSyncActivityTypes(unittest.TestCase):
    def setUp(self):
        self.client = Client("123-ABC-456", "id", "secret")
        self.client.token_expires = pendulum.utcnow().add(days=1)
        self.client.calls_today = 1
        self.stream = discover_catalog("activity_types")
        for schema in self.stream["schema"]["properties"].values():
            schema["selected"] = True

    @unittest.mock.patch("singer.write_record")
    def test_sync_activity_types(self, write_record):
        activity_type = {
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

        data = {
            "success": True,
            "result": [activity_type],
        }

        with requests_mock.Mocker(real_http=True) as mock:
            mock.register_uri("GET", self.client.get_url("rest/v1/activities/types.json"), json=data)
            sync_activity_types(self.client, {}, self.stream)

        write_record.assert_called_once_with("activity_types", activity_type)


class TestSyncPaginated(unittest.TestCase):
    def setUp(self):
        self.client = Client("123-ABC-456", "id", "secret")
        self.client.token_expires = pendulum.utcnow().add(days=1)
        self.client.calls_today = 1
        self.stream = discover_catalog("programs")

    def test_sync_paginated(self):
        state = {"bookmarks": {"programs": {"updatedAt": "2017-01-01T00:00:00Z", "next_page_token": "abc"}}}
        endpoint = "rest/v1/programs.json"
        url = self.client.get_url(endpoint)

        responses = [
            {"json": {"success": True, "result": [], "nextPageToken": "def"}, "status_code": 200},
            {"json": {"success": True, "result": []}, "status_code": 200},
        ]

        with requests_mock.Mocker(real_http=True) as mock:
            matcher = mock.register_uri("GET", url, responses)
            sync_paginated(self.client, state, self.stream)

        # We should have made 2 requests
        self.assertEqual(2, len(matcher.request_history))

        # Assert that we used the paging token from state
        self.assertDictEqual({"batchsize": "300", "nextpagetoken": "abc"}, parse_params(matcher.request_history[0]))
        self.assertDictEqual({"batchsize": "300", "nextpagetoken": "def"}, parse_params(matcher.request_history[1]))

    def test_sync_paginated_fail(self):
        state = {"bookmarks": {"programs": {"updatedAt": "2017-01-01T00:00:00Z", "next_page_token": "abc"}}}
        endpoint = "rest/v1/programs.json"
        url = self.client.get_url(endpoint)

        # The empty 200 is an actual Marketo response to bad requests
        responses = [
            {"json": {"success": True, "result": [], "nextPageToken": "def"}, "status_code": 200},
            {"text": "", "status_code": 200},
        ]

        with requests_mock.Mocker(real_http=True) as mock:
            matcher = mock.register_uri("GET", url, responses)
            with self.assertRaises(ApiException):
                sync_paginated(self.client, state, self.stream)

        # The last paging token should still be there
        self.assertEqual("def", state["bookmarks"]["programs"]["next_page_token"])


class TestSyncActivities(unittest.TestCase):
    def setUp(self):
        self.client = Client("123-ABC-456", "id", "secret")
        self.client.token_expires = pendulum.utcnow().add(days=1)
        self.client.calls_today = 1
        self.stream = {
            "tap_stream_id": "activities_1",
            "stream": "activities_1",
            "key_properties": ["marketoGUID"],
            "replication_key": "activityDate",
            "replication_method": "INCREMENTAL",
            "schema": {
                "type": "object",
                "additionalProperties": False,
                "inclusion": "available",
                "selected": True,
                "properties": {
                    "marketoGUID": {
                        "type": "string",
                        "inclusion": "automatic",
                        "selected": True,
                    },
                    "leadId": {
                        "type": "integer",
                        "inclusion": "automatic",
                        "selected": True,
                    },
                    "activityDate": {
                        "type": "string",
                        "format": "date-time",
                        "inclusion": "automatic",
                        "selected": True,
                    },
                    "activityTypeId": {
                        "type": "integer",
                        "inclusion": "automatic",
                        "selected": True,
                    },
                    "webpage_id": {
                        "type": "integer",
                        "inclusion": "automatic",
                        "selected": True,
                    },
                    "client_ip_address": {
                        "type": "string",
                        "inclusion": "available",
                        "selected": True,
                    },
                    "query_parameters": {
                        "type": "string",
                        "inclusion": "available",
                        "selected": False,
                    },
                }
            }
        }

    def test_format_values(self):
        row = {
            "marketoGUID": "abc123",
            "leadId": "123",
            "activityDate": "2017-01-01T00:00:00Z",
            "activityTypeId": "1",
            "webpage_id": "123",
            "client_ip_address": "0.0.0.0",
            "query_parameters": "",
        }
        expected = {
            "marketoGUID": "abc123",
            "leadId": 123,
            "activityDate": "2017-01-01T00:00:00+00:00",
            "activityTypeId": 1,
            "webpage_id": 123,
            "client_ip_address": "0.0.0.0",
        }
        self.assertDictEqual(expected, format_values(self.stream, row))

    def test_flatten_activity(self):
        row = {
            "marketoGUID": "abc123",
            "leadId": "123",
            "activityDate": "2017-01-01T00:00:00Z",
            "activityTypeId": "1",
            "primaryAttributeValue": "123",
            "primaryAttributeValueId": "",
            "attributes": json.dumps({
                "Client IP Address": "0.0.0.0",
                "Query Parameters": "",
            }),
        }
        expected = {
            "marketoGUID": "abc123",
            "leadId": "123",
            "activityDate": "2017-01-01T00:00:00Z",
            "activityTypeId": "1",
            "webpage_id": "123",
            "client_ip_address": "0.0.0.0",
            "query_parameters": "",
        }
        self.assertDictEqual(expected, flatten_activity(row, self.stream["schema"]))

    def test_get_or_create_export_get_export_id(self):
        state = {"bookmarks": {"activities_1": {"export_id": "123", "export_end": "2017-01-01T00:00:00Z"}}}
        self.assertEqual("123", get_or_create_export(self.client, state, self.stream))

    @freezegun.freeze_time("2017-01-15")
    def test_get_or_create_export_create_export(self):
        state = {"bookmarks": {"activities_1": {"activityDate": "2017-01-01T00:00:00+00:00"}}}
        self.client.create_export = unittest.mock.MagicMock(return_value="123")

        # Ensure we got the right export id back
        self.assertEqual("123", get_or_create_export(self.client, state, self.stream))

        # Ensure that we called create export with the correct args
        expected_query = {"createdAt": {"startAt": "2017-01-01T00:00:00+00:00",
                                        "endAt": "2017-01-15T00:00:00+00:00"},
                          "activityTypeIds": ["1"]}
        self.client.create_export.assert_called_once_with("activities", ACTIVITY_FIELDS, expected_query)

        # Ensure state was updated
        expected_state = {"bookmarks": {"activities_1": {"activityDate": "2017-01-01T00:00:00+00:00",
                                                         "export_id": "123",
                                                         "export_end": "2017-01-15T00:00:00+00:00"}}}
        self.assertDictEqual(expected_state, state)

    @unittest.mock.patch("singer.write_record")
    def test_handle_record(self, write_record):
        state = {"bookmarks": {"activities_1": {"activityDate": "2017-01-01T00:00:00Z"}}}
        record = {"activityDate": "2017-01-02T00:00:00+00:00"}
        self.assertEqual(1, handle_record(state, self.stream, record))
        write_record.assert_called_once_with("activities_1", record)

    @unittest.mock.patch("singer.write_record")
    def test_handle_record_rejected(self, write_record):
        state = {"bookmarks": {"activities_1": {"activityDate": "2017-01-01T00:00:00Z"}}}
        record = {"activityDate": "2016-01-01T00:00:00+00:00"}
        self.assertEqual(0, handle_record(state, self.stream, record))
        write_record.assert_not_called()

    def test_sync_activites(self):
        pass
