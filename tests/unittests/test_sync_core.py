"""Unit tests for sync helper primitives and stream routing orchestration."""

from types import SimpleNamespace
import unittest
from unittest.mock import MagicMock, patch
import importlib

sync_module = importlib.import_module("tap_marketo.sync")


class DummyCounter:
    """Minimal stand-in for Singer counters used in sync routing tests."""

    def __init__(self):
        self.value = 0

    def _pop(self):
        return None


class TestSyncHelpers(unittest.TestCase):
    """Validates replication key resolution, formatting, and state helpers."""

    @patch("tap_marketo.sync.pendulum.now")
    @patch("tap_marketo.sync.pendulum.utcnow", side_effect=AttributeError("utcnow"))
    def test_utcnow_falls_back_to_pendulum_now(self, _mock_utcnow, mock_now):
        """Confirms sync.utcnow falls back to pendulum.now("UTC") when needed."""
        sentinel = object()
        mock_now.return_value = sentinel

        self.assertIs(sentinel, sync_module.utcnow())
        mock_now.assert_called_once_with("UTC")

    def test_determine_replication_key(self):
        """Verifies replication key selection for activity, incremental, and full-table streams."""
        self.assertEqual("activityDate", sync_module.determine_replication_key("activities_open_email"))
        self.assertEqual("updatedAt", sync_module.determine_replication_key("leads"))
        self.assertEqual("updatedAt", sync_module.determine_replication_key("lists"))
        self.assertEqual("updatedAt", sync_module.determine_replication_key("programs"))
        self.assertIsNone(sync_module.determine_replication_key("activity_types"))
        self.assertIsNone(sync_module.determine_replication_key("unknown"))

    @patch("tap_marketo.sync.singer.log_warning")
    def test_format_value_paths(self, _log_warning):
        """Covers type coercion and date-time formatting for diverse schema types."""
        self.assertIsNone(sync_module.format_value("", {"type": "string"}))
        self.assertEqual("2024-01-01T00:00:00+00:00", sync_module.format_value("2024-01-01T00:00:00Z", {"type": "string", "format": "date-time"}))
        self.assertEqual(10, sync_module.format_value("10.5", {"type": "integer"}))
        self.assertEqual(4.2, sync_module.format_value("4.2", {"type": "number"}))
        self.assertTrue(sync_module.format_value(True, {"type": "boolean"}))
        self.assertTrue(sync_module.format_value("true", {"type": "boolean"}))
        self.assertEqual("1", sync_module.format_value(1, {"type": "string"}))
        self.assertEqual("raw", sync_module.format_value("raw", {"type": ["object", "null"]}))

    def test_get_available_fields_and_format_values(self):
        """Ensures selected/automatic fields are emitted and non-selected fields are skipped."""
        stream = {
            "metadata": [
                {"breadcrumb": [], "metadata": {}},
                {"breadcrumb": ["properties", "id"], "metadata": {"inclusion": "automatic"}},
                {"breadcrumb": ["properties", "name"], "metadata": {"selected": True}},
                {"breadcrumb": ["properties", "skip_me"], "metadata": {"selected": False}},
            ],
            "schema": {
                "properties": {
                    "id": {"type": "integer"},
                    "name": {"type": "string"},
                    "skip_me": {"type": "string"},
                }
            },
        }
        row = {"id": "1", "name": "Alice", "skip_me": "x"}

        available_fields = sync_module.get_available_fields(stream)
        self.assertSetEqual({"id", "name"}, available_fields)

        formatted = sync_module.format_values(stream, row, available_fields)
        self.assertEqual({"id": 1, "name": "Alice"}, formatted)

        formatted_with_default = sync_module.format_values(stream, row)
        self.assertEqual({"id": 1, "name": "Alice"}, formatted_with_default)

    @patch("tap_marketo.sync.singer.write_state")
    def test_update_state_with_export_info(self, _write_state):
        """Validates export state bookkeeping persists export id/end and replication key."""
        state = {"bookmarks": {}}
        stream = {"tap_stream_id": "leads"}

        updated = sync_module.update_state_with_export_info(
            state,
            stream,
            bookmark="2024-01-02T00:00:00Z",
            export_id="123",
            export_end="2024-01-03T00:00:00Z",
        )

        lead_bookmarks = updated["bookmarks"]["leads"]
        self.assertEqual("123", lead_bookmarks["export_id"])
        self.assertEqual("2024-01-03T00:00:00Z", lead_bookmarks["export_end"])
        self.assertEqual("2024-01-02T00:00:00Z", lead_bookmarks["updatedAt"])

    def test_flatten_activity(self):
        """Verifies activity payload flattening including attributes and primary fields."""
        row = {
            "marketoGUID": "g",
            "leadId": 1,
            "activityDate": "2024-01-01T00:00:00Z",
            "activityTypeId": 2,
            "campaignId": 3,
            "primaryAttributeValue": "x",
            "primaryAttributeValueId": "y",
            "attributes": '{"Client IP Address": "127.0.0.1"}',
        }

        flattened = sync_module.flatten_activity(row, "primary_name")

        self.assertEqual("primary_name", flattened["primary_attribute_name"])
        self.assertEqual("x", flattened["primary_attribute_value"])
        self.assertEqual("y", flattened["primary_attribute_value_id"])
        self.assertEqual("127.0.0.1", flattened["client_ip_address"])

    def test_catalog_object_not_supported(self):
        """Ensures unsupported catalog object shapes raise a clear TypeError."""
        client = SimpleNamespace(use_corona=True)
        state = {"bookmarks": {"campaigns": {"updatedAt": "2023-01-01T00:00:00Z"}}}
        stream_obj = SimpleNamespace(
            tap_stream_id="campaigns",
            metadata=[SimpleNamespace(breadcrumb=(), metadata={"selected": True})],
            schema={"properties": {}},
            key_properties=["id"],
        )
        catalog_obj = SimpleNamespace(streams=[stream_obj])

        with self.assertRaises(TypeError):
            sync_module.sync(client, catalog_obj, {}, state)


class TestSyncRouting(unittest.TestCase):
    """Covers sync stream-selection logic and per-stream dispatch behavior."""

    def _stream(self, tap_stream_id, selected=True):
        return {
            "tap_stream_id": tap_stream_id,
            "metadata": [{"breadcrumb": [], "metadata": {"selected": selected}}],
            "schema": {"properties": {}},
            "key_properties": ["id"],
        }

    @patch("tap_marketo.sync.singer.write_state")
    @patch("tap_marketo.sync.singer.metrics.record_counter", return_value=DummyCounter())
    @patch("tap_marketo.sync.sync_paginated", return_value=({"bookmarks": {}}, 2))
    @patch("tap_marketo.sync.sync_leads", return_value=({"bookmarks": {}}, 1))
    def test_sync_skips_until_currently_syncing(self, _sync_leads, _sync_paginated, _counter, _write_state):
        """Verifies sync resumes from currently_syncing and skips earlier streams."""
        client = SimpleNamespace(use_corona=True)
        state = {"currently_syncing": "campaigns", "bookmarks": {}}
        catalog = {
            "streams": [
                self._stream("leads", selected=True),
                self._stream("campaigns", selected=True),
            ]
        }

        sync_module.sync(client, catalog, {}, state)

        _sync_leads.assert_not_called()
        _sync_paginated.assert_called_once()

    @patch("tap_marketo.sync.singer.log_warning")
    @patch("tap_marketo.sync.singer.write_state")
    @patch("tap_marketo.sync.singer.metrics.record_counter", return_value=DummyCounter())
    @patch("tap_marketo.sync.sync_leads", return_value=({"bookmarks": {}}, 1))
    def test_sync_logs_corona_warning_for_leads_without_corona(self, _sync_leads, _counter, _write_state, log_warning):
        """Ensures a warning is logged when leads sync runs without Corona support."""
        client = SimpleNamespace(use_corona=False)
        state = {"bookmarks": {}}
        catalog = {"streams": [self._stream("leads", selected=True)]}

        sync_module.sync(client, catalog, {}, state)

        log_warning.assert_called_once_with(sync_module.NO_CORONA_WARNING)

    @patch("tap_marketo.sync.singer.write_state")
    def test_sync_raises_for_unimplemented_stream(self, _write_state):
        """Ensures unknown selected streams fail fast with an exception."""
        client = SimpleNamespace(use_corona=True)
        state = {"bookmarks": {}}
        catalog = {"streams": [self._stream("not_implemented", selected=True)]}

        with self.assertRaises(Exception):
            sync_module.sync(client, catalog, {}, state)

    @patch("tap_marketo.sync.singer.log_info")
    def test_sync_logs_and_skips_unselected_stream(self, log_info):
        """Verifies unselected streams are logged and skipped without syncing."""
        client = SimpleNamespace(use_corona=True)
        state = {"bookmarks": {}}
        catalog = {"streams": [self._stream("campaigns", selected=False)]}

        sync_module.sync(client, catalog, {}, state)

        log_info.assert_any_call("%s: not selected", "campaigns")

    @patch("tap_marketo.sync.singer.write_state")
    @patch("tap_marketo.sync.singer.metrics.record_counter", return_value=DummyCounter())
    @patch("tap_marketo.sync.sync_programs", return_value=({"bookmarks": {}}, 3))
    @patch("tap_marketo.sync.sync_activities", return_value=({"bookmarks": {}}, 2))
    @patch("tap_marketo.sync.sync_activity_types", return_value=({"bookmarks": {}}, 1))
    def test_sync_routes_activity_types_activities_and_programs(
            self,
            mock_sync_activity_types,
            mock_sync_activities,
            mock_sync_programs,
            _counter,
            _write_state):
        """Verifies sync routes activity_types, activities, and programs to their handlers."""
        client = SimpleNamespace(use_corona=True)
        state = {"bookmarks": {"activities_open_email": {"activityDate": "2023-01-01T00:00:00Z"}}}
        catalog = {
            "streams": [
                self._stream("activity_types", selected=True),
                self._stream("activities_open_email", selected=True),
                self._stream("programs", selected=True),
            ]
        }

        sync_module.sync(client, catalog, {}, state)

        mock_sync_activity_types.assert_called_once()
        mock_sync_activities.assert_called_once()
        mock_sync_programs.assert_called_once()
