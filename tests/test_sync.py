import unittest
import unittest.mock
import urllib.parse

import freezegun
import pendulum
import requests_mock

from tap_marketo.client import Client, ApiException
from tap_marketo.discover import (discover_catalog,
                                  ACTIVITY_TYPES_AUTOMATIC_INCLUSION,
                                  ACTIVITY_TYPES_UNSUPPORTED,
                                  PROGRAMS_AUTOMATIC_INCLUSION)
from tap_marketo.sync import *


SAMPLE_LEADS_CSV = """
"""

def parse_params(request):
    return dict(urllib.parse.parse_qsl(request.query))


def fake_stream_rows(client, stream_id, export_id):
    for i in range(1000):
        yield {
            "id": str(i),
            "email": "test@email.com",
            "firstName": "John",
            "lastName": "Doe",
            "title": "Corpse",
            "company": "Morgue",
            "probConvert": "54",
            "createdAt": "2020-02-02T00:00:00Z",
            "updatedAt": "2021-02-02T00:00:00Z",
            "signedUpAt": "2020-12-02T00:00:00Z",
            "hasSignedUp": "TRUE"
        }


def fake_get_or_create_export_for_leads(
        client,
        state,
        stream,
        export_start,
        config
    ): 
    return (
        'xxx',
        min(export_start.add(days=30),
            pendulum.utcnow()
        ).replace(microsecond=0)
    )


class TestMarketoExport(unittest.TestCase):
    def setUp(self):
        self.client = Client("123-ABC-789", "id", "secret")
        self.client._use_corona = True
        self.sample_leads_csv = SAMPLE_LEADS_CSV
        self.mock_status_completed = {
            "result": [
                {
                    "exportId": "123",
                    "status": "Completed",
                    "createdAt": "2017-01-21T11:47:30-08:00",
                    "queuedAt": "2017-01-21T11:48:30-08:00",
                    "startedAt": "2017-01-21T11:51:30-08:00",
                    "finishedAt": "2017-01-21T12:59:30-08:00",
                    "format": "CSV",
                    "numberOfRecords": 3,
                    "fileSize": 123424,
                }
            ]
            }
        self.stream_leads = {
            "tap_stream_id": "leads",
            "schema": {
                "properties": {
                    "id": {"type": "integer"},
                    "email": {"type": ["null", "string"]},
                    "firstName": {"type": ["null", "string"]},
                    "lastName": {"type": ["null", "string"]},
                    "title": {"type": ["null", "string"]},
                    "company": {"type": ["null", "string"]},
                    "probConvert": {"type": ["null", "integer"]},
                    "createdAt": {"type": ["null", "string"], "format": "date-time"},
                    "updatedAt": {"type": ["null", "string"], "format": "date-time"},
                    "signedUpAt": {"type": ["null", "string"], "format": "date-time"},
                    "hasSignedUp": {"type": "boolean"}
                }
            },
            "metadata": [
                {"breadcrumb": ["properties", "id"], "metadata": {"inclusion": "automatic"}},
                {"breadcrumb": ["properties", "email"], "metadata": {"selected": False}},
                {"breadcrumb": ["properties", "firstName"], "metadata": {"selected": True}},
                {"breadcrumb": ["properties", "lastName"], "metadata": {"selected": True}},
                {"breadcrumb": ["properties", "title"], "metadata": {"selected": True}},
                {"breadcrumb": ["properties", "company"], "metadata": {"selected": True}},
                {"breadcrumb": ["properties", "probConvert"], "metadata": {"selected": True}},
                {"breadcrumb": ["properties", "createdAt"], "metadata": {"inclusion": "automatic"}},
                {"breadcrumb": ["properties", "updatedAt"], "metadata": {"inclusion": "automatic"}},
                {"breadcrumb": ["properties", "updatedAt"], "metadata": {"selected": False}},
                {"breadcrumb": ["properties", "hasSignedUp"], "metadata": {"selected": "True"}}
            ],
            "key_properties": ["id"],
        }

    def test_format_value(self):
        test_cases = [
            ('', {"type": {"null", "string"}}, None),
            ('null', {"type": {"null", "string"}}, None),
            ("True", {"type": {"null", "boolean"}}, True),
            ("123", {"type": {"null", "string"}}, "123"),
            ("123.123", {"type": {"null", "integer"}}, 123),
            ("123", {"type": {"null", "integer"}}, 123),
            ("12.3", {"type": {"null", "number"}}, 12.3),
            ("123", {"type": {"null", "number"}}, 123.0)
        ]
        for input_value, input_type, expected_output_value in test_cases:
            self.assertEqual(expected_output_value, format_value(input_value, input_type))

    def test_format_values(self):
        """Test format_values converts types & filters allowed attributes"""
        expected = {
            "id": 1,
            "firstName": "John",
            "lastName": "Doe",
            "title": "Corpse",
            "company": "Morgue",
            "probConvert": 54,
            "createdAt": "2020-02-02T00:00:00+00:00",
            "updatedAt": "2021-02-02T00:00:00+00:00",
            "hasSignedUp": True
        }
        row = {
            "id": "1",
            "email": "test@email.com",
            "firstName": "John",
            "lastName": "Doe",
            "title": "Corpse",
            "company": "Morgue",
            "probConvert": "54",
            "createdAt": "2020-02-02T00:00:00Z",
            "updatedAt": "2021-02-02T00:00:00Z",
            "signedUpAt": "2020-12-02T00:00:00Z",
            "hasSignedUp": "TRUE"
        }
        output_schema = get_output_schema(self.stream_leads)

        actual = format_values(output_schema, row)
        self.assertEqual(expected, actual)
        
    def test_get_output_schema(self):
        """Test get expected schema filters only selected/automatic fields & puts type in a list"""
        actual = get_output_schema(self.stream_leads)
        expected = {
                "id": {"type": ["integer"]},
                "firstName": {"type": ["null", "string"]},
                "lastName": {"type": ["null", "string"]},
                "title": {"type": ["null", "string"]},
                "company": {"type": ["null", "string"]},
                "probConvert": {"type": ["null", "integer"]},
                "createdAt": {"type": ["null", "string"], "format": "date-time"},
                "updatedAt": {"type": ["null", "string"], "format": "date-time"},
                "hasSignedUp": {"type": ["boolean"]}
            }
        self.assertEqual(str(actual), str(expected))

    @unittest.mock.patch("singer.bookmarks.get_bookmark")
    @unittest.mock.patch("tap_marketo.sync.wait_for_export")
    @unittest.mock.patch("tap_marketo.sync.update_state_with_export_info")
    @unittest.mock.patch("singer.write_schema")
    @unittest.mock.patch("singer.write_record")
    def test_sync_leads_creates_2_export_jobs(
        self,
        write_record,
        write_schema,
        update_state_with_export_info,
        wait_for_export,
        get_bookmark
        ):
        """Test sync leads creates only 2 jobs / extracts 2000 rows"""

        get_bookmark.side_effect = [
            pendulum.now('UTC').subtract(days=40).isoformat(),
            pendulum.now('UTC').subtract(days=40).isoformat()
        ]

        state = "foobar"
        config = {}
        with unittest.mock.patch(
            'tap_marketo.sync.get_or_create_export_for_leads',
            wraps=fake_get_or_create_export_for_leads
        ), unittest.mock.patch(
            'tap_marketo.sync.stream_rows',
            wraps=fake_stream_rows
        ):
            state, record_count = sync_leads(
                self.client,
                state,
                self.stream_leads,
                config
            )
            self.assertEqual(record_count, 2000)


class MockResponse:
    def __init__(self, data):
        self.data = data
    def iter_content(self, decode_unicode=True, chunk_size=512):
        yield self.data
    def iter_lines(self, decode_unicode=True, chunk_size=512):
        yield self.data

# class TestSyncActivityTypes(unittest.TestCase):
#     def setUp(self):
#         self.client = Client("123-ABC-456", "id", "secret")
#         self.client.token_expires = pendulum.utcnow().add(days=1)
#         self.client.calls_today = 1
#         self.stream = discover_catalog("activity_types", ACTIVITY_TYPES_AUTOMATIC_INCLUSION, unsupported=ACTIVITY_TYPES_UNSUPPORTED, stream_acutomatic_inclusion=True)
#         for schema in self.stream["schema"]["properties"].values():
#             schema["selected"] = True

#     @unittest.mock.patch("singer.write_record")
#     def test_sync_activity_types(self, write_record):
#         activity_type = {
#             "id": 1,
#             "name": "Visit Webpage",
#             "description": "User visits a web page",
#             "primaryAttribute": {
#                 "name": "Webpage ID",
#                 "dataType": "integer",
#             },
#             "attributes": [
#                 {
#                     "name": "Client IP Address",
#                     "dataType": "string",
#                 },
#                 {
#                     "name": "Query Parameters",
#                     "dataType": "string",
#                 },
#             ],
#         }

#         data = {
#             "success": True,
#             "result": [activity_type],
#         }

#         with requests_mock.Mocker(real_http=True) as mock:
#             mock.register_uri("GET", self.client.get_url("rest/v1/activities/types.json"), json=data)
#             sync_activity_types(self.client, {}, self.stream)

#         write_record.assert_called_once_with("activity_types", activity_type)

# class TestSyncLeads(unittest.TestCase):
#     def mocked_client_create_export(self, stream_name, fields, query):
#         return 1234

#     def setUp(self):
#         self.client = Client("123-ABC-456", "id", "secret")
#         self.client.token_expires = pendulum.utcnow().add(days=1)
#         self.client.calls_today = 1
#         self.client._use_corona = True
#         self.stream = {"tap_stream_id": "leads",
#                        "key_properties": ["marketoGUID"],
#                        "replication_key": "updatedAt",
#                        "schema": {"properties": {
#                            "marketoGUID": {
#                                "type": "string",
#                                "inclusion": "automatic",
#                                "selected": True,
#                            },
#                            "id": {
#                                "type": "integer",
#                                "inclusion": "automatic",
#                                "selected": False,
#                            },
#                            "updatedAt": {
#                                "type": "string",
#                                "format": "date-time",
#                                "inclusion": "automatic",
#                                "selected": False,
#                            },
#                            "attributes": {
#                                "type": "string",
#                                "inclusion": "available",
#                                "selected": True,
#                            }}}}

#         self.client.create_export = self.mocked_client_create_export

#     @freezegun.freeze_time("2017-01-15")
#     def test_get_or_create_export_resume(self):
#         export_end = pendulum.now().add(days=30).isoformat()
#         mock_state = {"bookmarks":
#                       {"leads":
#                        {"updatedAt": pendulum.now().isoformat(),
#                         "export_id": 5678,
#                         "export_end": export_end}}}

#         fields = self.stream["schema"]["properties"].values()
#         query = {"updatedAt": {"startAt": pendulum.now().isoformat(), "endAt": pendulum.now().add(days=30).isoformat()}}
#         export_info = get_or_create_export_for_leads(self.client, mock_state, \
#                                                    self.stream, \
#                                                      fields,
#                                                      pendulum.now())
#         self.assertEqual(export_info, (5678, pendulum.parse(export_end)))

#     @freezegun.freeze_time("2017-01-15")
#     @unittest.mock.patch("singer.write_record")
#     def test_write_leads_records_no_corona(self, write_record):
#         self.client._use_corona = False
#         mock_state = {"bookmarks":
#                       {"leads":
#                        {"updatedAt": "2017-01-01"}}}

#         mock_lines = 'id,a,b,updatedAt\n1,1,2,2017-01-16\n1,1,2,2017-01-01'
#         mocked_resp = MockResponse(mock_lines)

#         mock_og_value = pendulum.now()
#         mock_record_count = 0
#         record_count = write_leads_records(self.client, self.stream, \
#                                            mocked_resp, mock_og_value, mock_record_count)

#         self.assertEqual(record_count, 1)


#     @freezegun.freeze_time("2017-01-15")
#     @unittest.mock.patch("singer.write_record")
#     def test_write_leads_allow_null_updated_at(self, write_record):
#         self.client._use_corona = False
#         mock_state = {"bookmarks":
#                       {"leads":
#                        {"updatedAt": "2017-01-01"}}}


#         mock_lines = 'id,a,b,updatedAt\n1,1,2,2017-01-16\n1,1,2,null'
#         mocked_resp = MockResponse(mock_lines)

#         mock_og_value = pendulum.now()
#         mock_record_count = 0
#         record_count = write_leads_records(self.client, self.stream, \
#                                            mocked_resp, mock_og_value, mock_record_count)

#         self.assertEqual(record_count, 2)

#     @unittest.mock.patch("singer.write_record")
#     @freezegun.freeze_time("2017-01-15")
#     def test_sync_leads(self, write_record):
#         self.client._use_corona = False
#         state = {"bookmarks": {"leads": {"updatedAt": "2017-01-01T00:00:00+00:00",
#                                          "export_id": "123",
#                                          "export_end": "2017-01-15T00:00:00+00:00"}}}
#         lines = 'marketoGUID,id,updatedAt,attributes\n1,1,2016-12-31T00:00:00+00:00,"1\n2"\n2,2,2017-01-01T00:00:00+00:00,"1"\n3,3,2017-01-02T00:00:00+00:00,"1"\n4,4,2017-01-03T00:00:00+00:00,"ab\nc"'

#         self.client.wait_for_export = unittest.mock.MagicMock(return_value=True)
#         self.client.stream_export = unittest.mock.MagicMock(return_value=MockResponse(lines))

#         state, record_count = sync_leads(self.client, state, self.stream)

#         # one record was too old, so we should have 3
#         self.assertEqual(3, record_count)

#         # export_end was the 15th, so the updatedAt date should be updated and no export
#         expected_state = {"bookmarks": {"leads": {"updatedAt": "2017-01-15T00:00:00+00:00",
#                                                   "export_id": None,
#                                                   "export_end": None}}}
#         self.assertDictEqual(expected_state, state)

#         expected_calls = [
#             unittest.mock.call("leads",
#                                {"marketoGUID": "2", "id": 2, "updatedAt": "2017-01-01T00:00:00+00:00", "attributes": "1"}),
#             unittest.mock.call("leads",
#                                {"marketoGUID": "3", "id": 3, "updatedAt": "2017-01-02T00:00:00+00:00", "attributes": "1"}),
#             unittest.mock.call("leads",
#                                {"marketoGUID": "4", "id": 4, "updatedAt": "2017-01-03T00:00:00+00:00", "attributes": "ab\nc"})
#         ]
#         write_record.assert_has_calls(expected_calls)

#     @unittest.mock.patch("singer.write_state")
#     @freezegun.freeze_time("2017-01-15")
#     def test_sync_leads_bad_csv(self, write_record):
#         self.client._use_corona = False
#         state = {"bookmarks": {"leads": {"updatedAt": "2017-01-01T00:00:00+00:00",
#                                          "export_id": "123",
#                                          "export_end": "2017-01-15T00:00:00+00:00"}}}
#         lines = [
#             b'marketoGUID,id,attributes',
#             b'1,1,1',
#             b'2,2,1',
#             b'3,3,1',
#             b'4,4,1'
#         ]

#         self.client.wait_for_export = unittest.mock.MagicMock(return_value=True)
#         self.client.stream_export = unittest.mock.MagicMock(return_value=(l for l in lines))

#         try:
#             state, record_count = sync_leads(self.client, state, self.stream)
#         except Exception:
#             pass


#         expected_calls = [
#             unittest.mock.call({"bookmarks": {"leads": {"updatedAt": "2017-01-01T00:00:00+00:00",
#                                                   "export_id": None,
#                                                   "export_end": None}}})]
#         singer.write_state.assert_has_calls(expected_calls)

# class TestSyncPaginated(unittest.TestCase):
#     def setUp(self):
#         self.client = Client("123-ABC-456", "id", "secret")
#         self.client.token_expires = pendulum.utcnow().add(days=1)
#         self.client.calls_today = 1
#         self.stream = discover_catalog("programs", PROGRAMS_AUTOMATIC_INCLUSION)

#     def test_sync_paginated(self):
#         state = {"bookmarks": {"programs": {"updatedAt": "2017-01-01T00:00:00Z", "next_page_token": "abc"}}}
#         endpoint = "rest/v1/programs.json"
#         url = self.client.get_url(endpoint)

#         responses = [
#             {"json": {"success": True, "result": [], "nextPageToken": "def"}, "status_code": 200},
#             {"json": {"success": True, "result": []}, "status_code": 200},
#         ]

#         with requests_mock.Mocker(real_http=True) as mock:
#             matcher = mock.register_uri("GET", url, responses)
#             sync_paginated(self.client, state, self.stream)

#         # We should have made 2 requests
#         self.assertEqual(2, len(matcher.request_history))

#         # Assert that we used the paging token from state
#         self.assertDictEqual({"batchsize": "300", "nextpagetoken": "abc"}, parse_params(matcher.request_history[0]))
#         self.assertDictEqual({"batchsize": "300", "nextpagetoken": "def"}, parse_params(matcher.request_history[1]))

#     def test_sync_paginated_fail(self):
#         state = {"bookmarks": {"programs": {"updatedAt": "2017-01-01T00:00:00Z", "next_page_token": "abc"}}}
#         endpoint = "rest/v1/programs.json"
#         url = self.client.get_url(endpoint)

#         # The empty 200 is an actual Marketo response to bad requests
#         responses = [
#             {"json": {"success": True, "result": [], "nextPageToken": "def"}, "status_code": 200},
#             {"text": "", "status_code": 200},
#         ]

#         with requests_mock.Mocker(real_http=True) as mock:
#             matcher = mock.register_uri("GET", url, responses)
#             with self.assertRaises(ApiException):
#                 sync_paginated(self.client, state, self.stream)

#         # The last paging token should still be there
#         self.assertEqual("def", state["bookmarks"]["programs"]["next_page_token"])


# class TestSyncActivities(unittest.TestCase):
#     def setUp(self):
#         self.client = Client("123-ABC-456", "id", "secret")
#         self.client.token_expires = pendulum.utcnow().add(days=1)
#         self.client.calls_today = 1
#         self.stream = {
#             "tap_stream_id": "activities_activity_name",
#             "stream": "activities_activity_name",
#             "key_properties": ["marketoGUID"],
#             "metadata": [{'breadcrumb': (),
#                           'metadata': {'marketo.activity-id': 1,
#                                        'marketo.primary-attribute-name': 'webpage_id'}}],
#             "schema": {
#                 "type": "object",
#                 "additionalProperties": False,
#                 "inclusion": "available",
#                 "selected": True,
#                 "properties": {
#                     "marketoGUID": {
#                         "type": "string",
#                         "inclusion": "automatic",
#                         "selected": True,
#                     },
#                     "primary_attribute_name": {
#                         "type": "string",
#                         "inclusion": "automatic",
#                     },
#                     "primary_attribute_value_id": {
#                         "type": "string",
#                         "inclusion": "automatic",
#                     },
#                     "primary_attribute_value": {
#                         "type": "string",
#                         "inclusion": "automatic",
#                     },
#                     "leadId": {
#                         "type": "integer",
#                         "inclusion": "automatic",
#                         "selected": True,
#                     },
#                     "activityDate": {
#                         "type": "string",
#                         "format": "date-time",
#                         "inclusion": "automatic",
#                         "selected": True,
#                     },
#                     "activityTypeId": {
#                         "type": "integer",
#                         "inclusion": "automatic",
#                         "selected": True,
#                     },
#                     "client_ip_address": {
#                         "type": "string",
#                         "inclusion": "available",
#                         "selected": True,
#                     },
#                     "query_parameters": {
#                         "type": "string",
#                         "inclusion": "available",
#                         "selected": False,
#                     },
#                 }
#             }
#         }

#     def test_format_values(self):
#         row = {
#             "marketoGUID": "abc123",
#             "leadId": "123",
#             "activityDate": "2017-01-01T00:00:00Z",
#             "activityTypeId": "1",
#             "webpage_id": "123",
#             "primary_attribute_name": "Webpage Id",
#             "primary_attribute_value": "123",
#             "primary_attribute_value_id": None,
#             "client_ip_address": "0.0.0.0",
#             "query_parameters": "",
#         }
#         expected = {
#             "marketoGUID": "abc123",
#             "leadId": 123,
#             "activityDate": "2017-01-01T00:00:00+00:00",
#             "activityTypeId": 1,
#             "primary_attribute_name": "Webpage Id",
#             "primary_attribute_value": "123",
#             "primary_attribute_value_id": None,
#             "client_ip_address": "0.0.0.0",
#         }
#         self.assertDictEqual(expected, format_values(self.stream, row))

#     def test_flatten_activity(self):
#         row = {
#             "marketoGUID": "abc123",
#             "leadId": "123",
#             "activityDate": "2017-01-01T00:00:00Z",
#             "activityTypeId": "1",
#             "primaryAttributeValue": "123",
#             "primaryAttributeValueId": "",
#             "attributes": json.dumps({
#                 "Client IP Address": "0.0.0.0",
#                 "Query Parameters": "",
#             }),
#         }
#         expected = {
#             "marketoGUID": "abc123",
#             "leadId": "123",
#             "activityDate": "2017-01-01T00:00:00Z",
#             "activityTypeId": "1",
#             "client_ip_address": "0.0.0.0",
#             "query_parameters": "",
#             "primary_attribute_name": 'webpage_id',
#             "primary_attribute_value": "123",
#             "primary_attribute_value_id": ""
#         }
#         self.assertDictEqual(expected, flatten_activity(row, self.stream))

#     def test_get_or_create_export_get_export_id(self):
#         state = {"bookmarks": {"activities_activity_name": {"export_id": "123", "export_end": "2017-01-01T00:00:00Z"}}}
#         self.assertEqual("123", get_or_create_export_for_activities(self.client, state, self.stream))

#     @freezegun.freeze_time("2017-01-15")
#     def test_get_or_create_export_create_export(self):
#         state = {"bookmarks": {"activities_activity_name": {"activityDate": "2017-01-01T00:00:00+00:00"}}}
#         self.client.create_export = unittest.mock.MagicMock(return_value="123")

#         # Ensure we got the right export id back
#         self.assertEqual("123", get_or_create_export_for_activities(self.client, state, self.stream))

#         # Ensure that we called create export with the correct args
#         expected_query = {"createdAt": {"startAt": "2017-01-01T00:00:00+00:00",
#                                         "endAt": "2017-01-15T00:00:00+00:00"},
#                           "activityTypeIds": [1]}
#         self.client.create_export.assert_called_once_with("activities", ACTIVITY_FIELDS, expected_query)

#         # Ensure state was updated
#         expected_state = {"bookmarks": {"activities_activity_name": {"activityDate": "2017-01-01T00:00:00+00:00",
#                                                          "export_id": "123",
#                                                          "export_end": "2017-01-15T00:00:00+00:00"}}}
#         self.assertDictEqual(expected_state, state)

#     @unittest.mock.patch("singer.write_record")
#     def test_handle_record(self, write_record):
#         state = {"bookmarks": {"activities_activity_name": {"activityDate": "2017-01-01T00:00:00Z"}}}
#         record = {"activityDate": "2017-01-02T00:00:00+00:00"}
#         self.assertEqual(1, handle_record(state, self.stream, record))
#         write_record.assert_called_once_with("activities_activity_name", record)

#     @unittest.mock.patch("singer.write_record")
#     def test_handle_record_rejected(self, write_record):
#         state = {"bookmarks": {"activities_activity_name": {"activityDate": "2017-01-01T00:00:00Z"}}}
#         record = {"activityDate": "2016-01-01T00:00:00+00:00"}
#         self.assertEqual(0, handle_record(state, self.stream, record))
#         write_record.assert_not_called()

#     def test_wait_for_activity_export(self):
#         state = {"bookmarks": {"activities_activity_name": {"activityDate": "2017-01-01T00:00:00+00:00",
#                                                 "export_id": "123",
#                                                 "export_end": "2017-01-31:00:00+00:00"}}}
#         self.client.wait_for_export = unittest.mock.MagicMock(side_effect=ExportFailed())

#         with self.assertRaises(ExportFailed):
#             wait_for_activity_export(self.client, state, self.stream, "123")

#         expected_state = {"bookmarks": {"activities_activity_name": {"activityDate": "2017-01-01T00:00:00+00:00",
#                                                          "export_id": None,
#                                                          "export_end": None}}}
#         self.assertDictEqual(expected_state, state)

#     @unittest.mock.patch("singer.write_record")
#     @freezegun.freeze_time("2017-01-15")
#     def test_sync_activities(self, write_record):
#         state = {"bookmarks": {"activities_activity_name": {"activityDate": "2017-01-01T00:00:00+00:00",
#                                                 "export_id": "123",
#                                                 "export_end": "2017-01-15T00:00:00+00:00"}}}
#         lines = 'marketoGUID,leadId,activityDate,activityTypeId,primaryAttributeValue,primaryAttributeValueId,attributes\n1,1,2016-12-31T00:00:00+00:00,1,1,,{"Client IP Address":"0.0.0.0"}\n2,2,2017-01-01T00:00:00+00:00,1,1,,{"Client IP Address":"0.0.0.0"}\n3,3,2017-01-02T00:00:00+00:00,1,1,,{"Client IP Address":"0.0.0.0"}\n4,4,2017-01-03T00:00:00+00:00,1,1,,{"Client IP Address":"0.0.0.0"}'

#         self.client.wait_for_export = unittest.mock.MagicMock(return_value=True)
#         self.client.stream_export = unittest.mock.MagicMock(return_value=MockResponse(lines))

#         state, record_count = sync_activities(self.client, state, self.stream)

#         # one record was too old, so we should have 3
#         self.assertEqual(3, record_count)

#         # export_end was the 15th, so the activityDate should be updated and no export
#         expected_state = {"bookmarks": {"activities_activity_name": {"activityDate": "2017-01-15T00:00:00+00:00",
#                                                          "export_id": None,
#                                                          "export_end": None}}}
#         self.assertDictEqual(expected_state, state)

#         expected_calls = [
#             unittest.mock.call("activities_activity_name",
#                                {"marketoGUID": "2", "leadId": 2, "activityDate": "2017-01-01T00:00:00+00:00",
#                                 "activityTypeId": 1, "primary_attribute_value_id": None, "primary_attribute_name": "webpage_id", "primary_attribute_value": '1', "client_ip_address": "0.0.0.0"}),
#             unittest.mock.call("activities_activity_name",
#                                {"marketoGUID": "3", "leadId": 3, "activityDate": "2017-01-02T00:00:00+00:00",
#                                 "activityTypeId": 1, "primary_attribute_value_id": None, "primary_attribute_name": "webpage_id", "primary_attribute_value": '1', "client_ip_address": "0.0.0.0"}),
#             unittest.mock.call("activities_activity_name",
#                                {"marketoGUID": "4", "leadId": 4, "activityDate": "2017-01-03T00:00:00+00:00",
#                                 "activityTypeId": 1, "primary_attribute_value_id": None, "primary_attribute_name": "webpage_id", "primary_attribute_value": '1', "client_ip_address": "0.0.0.0"}),
#         ]
#         write_record.assert_has_calls(expected_calls)
