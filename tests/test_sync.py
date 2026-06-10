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


def parse_params(request):
    return dict(urllib.parse.parse_qsl(request.query))


class MockResponse:
    def __init__(self, data):
        self.data = data if isinstance(data, bytes) else data.encode('utf-8')
        self.closed = False
    def iter_content(self, decode_unicode=False, chunk_size=512):
        yield self.data
    def iter_lines(self, decode_unicode=False, chunk_size=512):
        yield self.data
    def close(self):
        self.closed = True


class TestSyncPrograms(unittest.TestCase):
    def setUp(self):
        self.client = Client("123-ABC-456", "id", "secret")
        self.client.token_expires = pendulum.utcnow().add(days=1)
        self.client.calls_today = 1
        self.stream = discover_catalog("programs", PROGRAMS_AUTOMATIC_INCLUSION)

    @unittest.mock.patch("singer.write_schema")
    @unittest.mock.patch("singer.write_state")
    def test_future_bookmark_returns_early_without_request(self, write_state, write_schema):
        # Bookmark is in the future — should return immediately with 0 records
        # and not call the Marketo API to avoid the 701 "End date should always
        # be after start date" error.
        future_date = pendulum.utcnow().add(days=1).isoformat()
        state = {"bookmarks": {"programs": {"updatedAt": future_date}}}

        with unittest.mock.patch.object(self.client, "request") as mock_request:
            returned_state, record_count = sync_programs(self.client, state, self.stream)

        mock_request.assert_not_called()
        self.assertEqual(0, record_count)
        self.assertEqual(state, returned_state)

    @unittest.mock.patch("singer.write_record")
    @unittest.mock.patch("singer.write_schema")
    @unittest.mock.patch("singer.write_state")
    @freezegun.freeze_time("2017-01-15")
    def test_past_bookmark_syncs_records(self, write_state, write_schema, write_record):
        # Bookmark is in the past — should request from Marketo and return records.
        state = {"bookmarks": {"programs": {"updatedAt": "2017-01-01T00:00:00+00:00"}}}

        program_row = {
            "id": 1,
            "name": "Test Program",
            "updatedAt": "2017-01-10T00:00:00+00:00",
            "createdAt": "2017-01-01T00:00:00+00:00",
        }
        page1 = {"success": True, "result": [program_row]}
        page2 = {"success": True, "warnings": [NO_ASSET_MSG], "result": []}

        with unittest.mock.patch.object(self.client, "request", side_effect=[page1, page2]):
            returned_state, record_count = sync_programs(self.client, state, self.stream)

        self.assertEqual(1, record_count)
        write_record.assert_called_once()
        self.assertEqual(
            "2017-01-15T00:00:00+00:00",
            returned_state["bookmarks"]["programs"]["updatedAt"],
        )

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


class TestIterStream(unittest.TestCase):
    def test_reads_single_chunk(self):
        chunks = iter([b'hello,world\n'])
        stream = IterStream(chunks)
        result = stream.read()
        self.assertEqual(b'hello,world\n', result)

    def test_reads_across_multiple_chunks(self):
        chunks = iter([b'hel', b'lo,', b'wor', b'ld\n'])
        stream = IterStream(chunks)
        result = stream.read()
        self.assertEqual(b'hello,world\n', result)

    def test_leftover_bytes_carried_forward(self):
        # readinto buf is smaller than the chunk — leftover must be preserved
        chunks = iter([b'abcdef'])
        stream = IterStream(chunks)
        buf = bytearray(4)
        n = stream.readinto(buf)
        self.assertEqual(4, n)
        self.assertEqual(b'abcd', bytes(buf))
        buf2 = bytearray(4)
        n2 = stream.readinto(buf2)
        self.assertEqual(2, n2)
        self.assertEqual(b'ef', bytes(buf2[:n2]))

    def test_returns_zero_on_exhausted_iterator(self):
        chunks = iter([])
        stream = IterStream(chunks)
        buf = bytearray(4)
        self.assertEqual(0, stream.readinto(buf))


class TestStreamRows(unittest.TestCase):
    def _make_mock_client(self, chunks):
        """Return a mock client whose stream_export yields the given byte chunks."""
        class MultiChunkResponse:
            def __init__(self, byte_chunks):
                self._chunks = byte_chunks
                self.closed = False
            def iter_content(self, decode_unicode=False, chunk_size=512):
                yield from self._chunks
            def close(self):
                self.closed = True

        client = unittest.mock.MagicMock()
        resp = MultiChunkResponse(chunks)
        client.stream_export.return_value = resp
        return client, resp

    def test_basic_csv_parsing(self):
        data = b'id,name\n1,Alice\n2,Bob\n'
        client, _ = self._make_mock_client([data])
        rows = list(stream_rows(client, 'leads', 'export-1'))
        self.assertEqual([{'id': '1', 'name': 'Alice'}, {'id': '2', 'name': 'Bob'}], rows)

    def test_cr_stripped(self):
        data = b'id,name\r\n1,Alice\r\n'
        client, _ = self._make_mock_client([data])
        rows = list(stream_rows(client, 'leads', 'export-1'))
        self.assertEqual([{'id': '1', 'name': 'Alice'}], rows)

    def test_null_bytes_stripped(self):
        data = b'id,name\n1,Ali\x00ce\n'
        client, _ = self._make_mock_client([data])
        rows = list(stream_rows(client, 'leads', 'export-1'))
        self.assertEqual([{'id': '1', 'name': 'Alice'}], rows)

    def test_response_closed_after_iteration(self):
        data = b'id\n1\n'
        client, resp = self._make_mock_client([data])
        list(stream_rows(client, 'leads', 'export-1'))
        self.assertTrue(resp.closed)

    def test_response_closed_on_exception(self):
        # Even if iteration raises, resp.close() must still be called.
        # An empty chunk produces no headers; next(reader) raises StopIteration,
        # which Python 3.7+ (PEP 479) converts to RuntimeError inside a generator.
        client, resp = self._make_mock_client([b''])
        try:
            list(stream_rows(client, 'leads', 'export-1'))
        except (StopIteration, RuntimeError):
            pass
        self.assertTrue(resp.closed)

    def test_multibyte_utf8_split_across_chunks(self):
        # 'café' encoded as UTF-8: b'caf\xc3\xa9' (\xc3\xa9 is a 2-byte sequence).
        # Split the chunk boundary between the two bytes of the multi-byte character.
        row = 'id,name\n1,café\n'.encode('utf-8')
        split = row.index(b'\xc3') + 1  # after \xc3, before \xa9
        chunk1, chunk2 = row[:split], row[split:]
        client, _ = self._make_mock_client([chunk1, chunk2])
        rows = list(stream_rows(client, 'leads', 'export-1'))
        self.assertEqual([{'id': '1', 'name': 'café'}], rows)
