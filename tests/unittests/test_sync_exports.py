"""Unit tests for export creation/reuse and endpoint-specific sync branches."""

import unittest
from unittest.mock import MagicMock, patch

import freezegun
import pendulum

from singer import metadata

from tap_marketo.client import ExportFailed
from tap_marketo.sync import (
    get_or_create_export_for_activities,
    get_or_create_export_for_leads,
    sync_activities,
    sync_activity_types,
    sync_leads,
    sync_paginated,
    wait_for_export,
)


def _selected_field_metadata(*fields):
    root = [{"breadcrumb": (), "metadata": {"selected": True}}]
    per_field = [
        {"breadcrumb": ("properties", field), "metadata": {"inclusion": "automatic", "selected": True}}
        for field in fields
    ]
    return root + per_field


class TestExportHelpers(unittest.TestCase):
    """Tests helper behavior for creating, reusing, and waiting on exports."""

    @patch("tap_marketo.sync.update_state_with_export_info", side_effect=lambda state, stream: state)
    def test_wait_for_export_clears_state_on_failure(self, _update_state):
        client = MagicMock()
        client.wait_for_export.side_effect = ExportFailed("failed")
        state = {"bookmarks": {}}
        stream = {"tap_stream_id": "activities_visit_webpage"}

        with self.assertRaises(ExportFailed):
            wait_for_export(client, state, stream, "exp-1")

        client.wait_for_export.assert_called_once_with("activities", "exp-1")
        _update_state.assert_called_once_with(state, stream)

    def test_wait_for_export_uses_leads_stream_type(self):
        client = MagicMock()
        state = {"bookmarks": {}}
        stream = {"tap_stream_id": "leads"}

        returned = wait_for_export(client, state, stream, "exp-2")

        self.assertEqual(state, returned)
        client.wait_for_export.assert_called_once_with("leads", "exp-2")

    @freezegun.freeze_time("2024-01-10")
    @patch("tap_marketo.sync.create_export_with_quota_backoff")
    @patch("tap_marketo.sync.update_state_with_export_info", side_effect=lambda state, stream, **kwargs: state)
    def test_get_or_create_export_for_leads_creates_new_export(self, _update_state, mock_backoff):
        client = MagicMock()
        client.use_corona = False
        stream = {
            "tap_stream_id": "leads",
            "metadata": _selected_field_metadata("id", "updatedAt"),
            "schema": {
                "properties": {
                    "id": {"type": "integer"},
                    "updatedAt": {"type": "string", "format": "date-time"},
                }
            },
        }
        state = {"bookmarks": {"leads": {"updatedAt": "2024-01-01T00:00:00+00:00"}}}
        export_start = pendulum.parse("2024-01-01T00:00:00+00:00")
        export_end = pendulum.parse("2024-01-05T00:00:00+00:00")

        def backoff_side_effect(create_fn, _start, _max_days):
            export_id = create_fn(export_end)
            return export_id, export_end

        mock_backoff.side_effect = backoff_side_effect
        client.create_export.return_value = "new-leads-export"

        export_id, returned_end = get_or_create_export_for_leads(client, state, stream, export_start, {})

        self.assertEqual("new-leads-export", export_id)
        self.assertEqual(export_end, returned_end)
        client.create_export.assert_called_once()
        create_call = client.create_export.call_args
        self.assertEqual("leads", create_call.args[0])
        self.assertIn("createdAt", create_call.args[2])
        _update_state.assert_called_once()

    def test_get_or_create_export_for_leads_reuses_existing_export(self):
        client = MagicMock()
        client.export_available.return_value = True
        state = {
            "bookmarks": {
                "leads": {
                    "updatedAt": "2024-01-01T00:00:00+00:00",
                    "export_id": "existing-export",
                    "export_end": "2024-01-04T00:00:00+00:00",
                }
            }
        }
        stream = {"tap_stream_id": "leads", "metadata": []}
        export_start = pendulum.parse("2024-01-01T00:00:00+00:00")

        export_id, export_end = get_or_create_export_for_leads(client, state, stream, export_start, {})

        self.assertEqual("existing-export", export_id)
        self.assertEqual("2024-01-04T00:00:00+00:00", export_end.isoformat())

    @patch("tap_marketo.sync.create_export_with_quota_backoff")
    @patch("tap_marketo.sync.update_state_with_export_info", side_effect=lambda state, stream, **kwargs: state)
    def test_get_or_create_export_for_leads_recreates_when_saved_export_unavailable(self, _update_state, mock_backoff):
        client = MagicMock()
        client.export_available.return_value = False
        client.use_corona = True
        stream = {
            "tap_stream_id": "leads",
            "metadata": _selected_field_metadata("id", "updatedAt"),
            "schema": {
                "properties": {
                    "id": {"type": "integer"},
                    "updatedAt": {"type": "string", "format": "date-time"},
                }
            },
        }
        state = {
            "bookmarks": {
                "leads": {
                    "updatedAt": "2024-01-01T00:00:00+00:00",
                    "export_id": "stale-export",
                    "export_end": "2024-01-02T00:00:00+00:00",
                }
            }
        }
        export_start = pendulum.parse("2024-01-01T00:00:00+00:00")
        export_end = pendulum.parse("2024-01-03T00:00:00+00:00")

        def backoff_side_effect(create_fn, _start, _max_days):
            export_id = create_fn(export_end)
            return export_id, export_end

        mock_backoff.side_effect = backoff_side_effect
        client.create_export.return_value = "recreated-export"

        export_id, _ = get_or_create_export_for_leads(client, state, stream, export_start, {})

        self.assertEqual("recreated-export", export_id)
        client.create_export.assert_called_once()

    @patch("tap_marketo.sync.create_export_with_quota_backoff")
    @patch("tap_marketo.sync.update_state_with_export_info", side_effect=lambda state, stream, **kwargs: state)
    def test_get_or_create_export_for_activities_uses_activity_type_id(self, _update_state, mock_backoff):
        client = MagicMock()
        client.export_available.return_value = True
        activity_mdata = metadata.to_list(metadata.write(metadata.new(), (), "marketo.activity-id", 33))
        stream = {
            "tap_stream_id": "activities_click_link",
            "metadata": activity_mdata,
        }
        state = {
            "bookmarks": {
                "activities_click_link": {"activityDate": "2024-01-01T00:00:00+00:00"}
            }
        }
        export_start = pendulum.parse("2024-01-01T00:00:00+00:00")
        export_end = pendulum.parse("2024-01-03T00:00:00+00:00")

        def backoff_side_effect(create_fn, _start, _max_days):
            export_id = create_fn(export_end)
            return export_id, export_end

        mock_backoff.side_effect = backoff_side_effect
        client.create_export.return_value = "activity-export"

        export_id, returned_end = get_or_create_export_for_activities(client, state, stream, export_start, {})

        self.assertEqual("activity-export", export_id)
        self.assertEqual(export_end, returned_end)
        create_call = client.create_export.call_args
        self.assertEqual("activities", create_call.args[0])
        self.assertEqual([33], create_call.args[2]["activityTypeIds"])

    def test_get_or_create_export_for_activities_reuses_existing_export(self):
        client = MagicMock()
        client.export_available.return_value = True
        stream = {
            "tap_stream_id": "activities_click_link",
            "metadata": metadata.to_list(metadata.write(metadata.new(), (), "marketo.activity-id", 33)),
        }
        state = {
            "bookmarks": {
                "activities_click_link": {
                    "activityDate": "2024-01-01T00:00:00+00:00",
                    "export_id": "existing-activity-export",
                    "export_end": "2024-01-04T00:00:00+00:00",
                }
            }
        }

        export_id, export_end = get_or_create_export_for_activities(
            client,
            state,
            stream,
            pendulum.parse("2024-01-01T00:00:00+00:00"),
            {},
        )

        self.assertEqual("existing-activity-export", export_id)
        self.assertEqual("2024-01-04T00:00:00+00:00", export_end.isoformat())

    @patch("tap_marketo.sync.create_export_with_quota_backoff")
    @patch("tap_marketo.sync.update_state_with_export_info", side_effect=lambda state, stream, **kwargs: state)
    def test_get_or_create_export_for_activities_recreates_when_unavailable(self, _update_state, mock_backoff):
        client = MagicMock()
        client.export_available.return_value = False
        activity_mdata = metadata.to_list(metadata.write(metadata.new(), (), "marketo.activity-id", 12))
        stream = {
            "tap_stream_id": "activities_fill_form",
            "metadata": activity_mdata,
        }
        state = {
            "bookmarks": {
                "activities_fill_form": {
                    "activityDate": "2024-01-01T00:00:00+00:00",
                    "export_id": "old-activity-export",
                    "export_end": "2024-01-02T00:00:00+00:00",
                }
            }
        }
        export_start = pendulum.parse("2024-01-01T00:00:00+00:00")
        export_end = pendulum.parse("2024-01-04T00:00:00+00:00")

        def backoff_side_effect(create_fn, _start, _max_days):
            export_id = create_fn(export_end)
            return export_id, export_end

        mock_backoff.side_effect = backoff_side_effect
        client.create_export.return_value = "fresh-activity-export"

        export_id, _ = get_or_create_export_for_activities(client, state, stream, export_start, {})

        self.assertEqual("fresh-activity-export", export_id)
        client.create_export.assert_called_once()


class TestSyncEndpointBranches(unittest.TestCase):
    """Validates lead/activity/paginated endpoint sync branch behavior."""

    @patch("tap_marketo.sync.singer.write_state")
    @patch("tap_marketo.sync.singer.write_schema")
    @patch("tap_marketo.sync.singer.write_record")
    @freezegun.freeze_time("2024-01-10")
    def test_sync_paginated_honors_start_date_and_clears_next_page(self, write_record, _write_schema, _write_state):
        client = MagicMock()
        client.request.side_effect = [
            {
                "result": [
                    {"id": 1, "updatedAt": "2024-01-02T00:00:00+00:00"},
                    {"id": 2, "updatedAt": "2023-12-31T00:00:00+00:00"},
                ],
                "nextPageToken": "next-token",
            },
            {
                "result": [
                    {"id": 3, "updatedAt": "2024-01-03T00:00:00+00:00"},
                ],
            },
        ]

        stream = {
            "tap_stream_id": "campaigns",
            "key_properties": ["id"],
            "metadata": _selected_field_metadata("id", "updatedAt"),
            "schema": {
                "properties": {
                    "id": {"type": "integer"},
                    "updatedAt": {"type": "string", "format": "date-time"},
                }
            },
        }
        state = {
            "bookmarks": {
                "campaigns": {
                    "updatedAt": "2024-01-01T00:00:00+00:00",
                    "next_page_token": "resume-token",
                }
            }
        }

        new_state, record_count = sync_paginated(client, state, stream)

        self.assertEqual(2, record_count)
        self.assertEqual(2, write_record.call_count)
        self.assertIsNone(new_state["bookmarks"]["campaigns"]["next_page_token"])
        self.assertEqual(
            "2024-01-10T00:00:00+00:00",
            new_state["bookmarks"]["campaigns"]["updatedAt"],
        )

    @patch("tap_marketo.sync.singer.write_schema")
    @patch("tap_marketo.sync.singer.write_record")
    def test_sync_activity_types_streams_rows(self, write_record, _write_schema):
        client = MagicMock()
        client.request.return_value = {
            "result": [
                {"id": 1, "name": "Visit Webpage"},
                {"id": 2, "name": "Fill Out Form"},
            ]
        }
        stream = {
            "tap_stream_id": "activity_types",
            "key_properties": ["id"],
            "metadata": _selected_field_metadata("id", "name"),
            "schema": {
                "properties": {
                    "id": {"type": "integer"},
                    "name": {"type": "string"},
                }
            },
        }

        returned_state, record_count = sync_activity_types(client, {"bookmarks": {}}, stream)

        self.assertEqual({"bookmarks": {}}, returned_state)
        self.assertEqual(2, record_count)
        self.assertEqual(2, write_record.call_count)

    @patch("tap_marketo.sync.singer.write_schema")
    @patch("tap_marketo.sync.singer.write_record")
    @patch("tap_marketo.sync.update_state_with_export_info", side_effect=lambda state, stream, **kwargs: state)
    @patch("tap_marketo.sync.wait_for_export", side_effect=lambda client, state, stream, export_id: state)
    @patch("tap_marketo.sync.get_or_create_export_for_leads")
    @freezegun.freeze_time("2024-01-10")
    def test_sync_leads_non_corona_filters_old_records(
            self,
            mock_get_or_create,
            _wait,
            _update,
            write_record,
            _write_schema):
        client = MagicMock()
        client.use_corona = False
        stream = {
            "tap_stream_id": "leads",
            "key_properties": ["id"],
            "metadata": _selected_field_metadata("id", "updatedAt"),
            "schema": {
                "properties": {
                    "id": {"type": "integer"},
                    "updatedAt": {"type": "string", "format": "date-time"},
                }
            },
        }
        state = {"bookmarks": {"leads": {"updatedAt": "2024-01-09T00:00:00+00:00"}}}
        export_end = pendulum.parse("2024-01-10T00:00:00+00:00")
        mock_get_or_create.return_value = ("lead-export", export_end)

        with patch("tap_marketo.sync.stream_rows", return_value=iter([
            {"id": "1", "updatedAt": "2024-01-08T00:00:00+00:00"},
            {"id": "2", "updatedAt": "2024-01-09T01:00:00+00:00"},
        ])):
            returned_state, record_count = sync_leads(client, state, stream, {})

        self.assertEqual(state, returned_state)
        self.assertEqual(1, record_count)
        write_record.assert_called_once()

    @patch("tap_marketo.sync.singer.write_schema")
    @patch("tap_marketo.sync.singer.write_record")
    @patch("tap_marketo.sync.update_state_with_export_info", side_effect=lambda state, stream, **kwargs: state)
    @patch("tap_marketo.sync.wait_for_export", side_effect=lambda client, state, stream, export_id: state)
    @patch("tap_marketo.sync.get_or_create_export_for_leads")
    @freezegun.freeze_time("2024-01-10")
    def test_sync_leads_corona_writes_all_records(
            self,
            mock_get_or_create,
            _wait,
            _update,
            write_record,
            _write_schema):
        client = MagicMock()
        client.use_corona = True
        stream = {
            "tap_stream_id": "leads",
            "key_properties": ["id"],
            "metadata": _selected_field_metadata("id", "updatedAt"),
            "schema": {
                "properties": {
                    "id": {"type": "integer"},
                    "updatedAt": {"type": "string", "format": "date-time"},
                }
            },
        }
        state = {"bookmarks": {"leads": {"updatedAt": "2024-01-09T00:00:00+00:00"}}}
        export_end = pendulum.parse("2024-01-10T00:00:00+00:00")
        mock_get_or_create.return_value = ("lead-export", export_end)

        with patch("tap_marketo.sync.stream_rows", return_value=iter([
            {"id": "1", "updatedAt": "2024-01-08T00:00:00+00:00"},
            {"id": "2", "updatedAt": "2024-01-09T01:00:00+00:00"},
        ])):
            returned_state, record_count = sync_leads(client, state, stream, {})

        self.assertEqual(state, returned_state)
        self.assertEqual(2, record_count)
        self.assertEqual(2, write_record.call_count)

    @patch("tap_marketo.sync.singer.write_schema")
    @patch("tap_marketo.sync.singer.write_record")
    @patch("tap_marketo.sync.update_state_with_export_info", side_effect=lambda state, stream, **kwargs: state)
    @patch("tap_marketo.sync.wait_for_export", side_effect=lambda client, state, stream, export_id: state)
    @patch("tap_marketo.sync.get_or_create_export_for_activities")
    @freezegun.freeze_time("2024-01-10")
    def test_sync_activities_streams_formatted_rows(
            self,
            mock_get_or_create,
            _wait,
            _update,
            write_record,
            _write_schema):
        client = MagicMock()
        stream_mdata = metadata.new()
        stream_mdata = metadata.write(stream_mdata, (), "marketo.primary-attribute-name", "webpage_id")
        stream = {
            "tap_stream_id": "activities_open_email",
            "key_properties": ["marketoGUID"],
            "metadata": metadata.to_list(stream_mdata) + _selected_field_metadata(
                "marketoGUID", "activityDate", "leadId", "activityTypeId", "campaignId", "primary_attribute_name", "primary_attribute_value", "primary_attribute_value_id", "client_ip_address"
            ),
            "schema": {
                "properties": {
                    "marketoGUID": {"type": "string"},
                    "activityDate": {"type": "string", "format": "date-time"},
                    "leadId": {"type": "integer"},
                    "activityTypeId": {"type": "integer"},
                    "campaignId": {"type": "integer"},
                    "primary_attribute_name": {"type": "string"},
                    "primary_attribute_value": {"type": "string"},
                    "primary_attribute_value_id": {"type": "string"},
                    "client_ip_address": {"type": "string"},
                }
            },
        }
        state = {"bookmarks": {"activities_open_email": {"activityDate": "2024-01-09T00:00:00+00:00"}}}
        export_end = pendulum.parse("2024-01-10T00:00:00+00:00")
        mock_get_or_create.return_value = ("activity-export", export_end)

        with patch("tap_marketo.sync.stream_rows", return_value=iter([
            {
                "marketoGUID": "g1",
                "leadId": "5",
                "activityDate": "2024-01-09T01:00:00+00:00",
                "activityTypeId": "2",
                "campaignId": "3",
                "primaryAttributeValue": "p",
                "primaryAttributeValueId": "pv",
                "attributes": '{"Client IP Address": "127.0.0.1"}',
            }
        ])):
            returned_state, record_count = sync_activities(client, state, stream, {})

        self.assertEqual(state, returned_state)
        self.assertEqual(1, record_count)
        write_record.assert_called_once()


if __name__ == "__main__":
    unittest.main()
