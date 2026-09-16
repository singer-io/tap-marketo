"""Unit tests for discover orchestration and catalog-building entry points."""

import unittest
from unittest.mock import MagicMock, patch

from tap_marketo.client import MarketoForbiddenError
from tap_marketo.discover import (
    CAMPAIGNS_AUTOMATIC_INCLUSION,
    discover,
    discover_catalog,
    discover_activities,
    discover_leads,
    check_stream_access,
    _get_probe_key,
)


class TestDiscover(unittest.TestCase):
    """Verifies discovery output shape and delegation behavior."""

    @patch("tap_marketo.discover.check_stream_access", return_value=True)
    @patch("tap_marketo.discover.discover_catalog")
    @patch("tap_marketo.discover.discover_activities")
    @patch("tap_marketo.discover.discover_leads")
    def test_discover_returns_catalog(self, mock_leads, mock_activities, mock_catalog, _mock_access):
        """Verifies discover() composes streams and returns a complete catalog payload."""
        mock_leads.return_value = {"tap_stream_id": "leads"}
        mock_activities.return_value = [{"tap_stream_id": "activities_visit_webpage"}]
        mock_catalog.side_effect = [
            {"tap_stream_id": "activity_types"},
            {"tap_stream_id": "campaigns"},
            {"tap_stream_id": "lists"},
            {"tap_stream_id": "programs"},
        ]

        result = discover(MagicMock())

        self.assertIn("streams", result)
        self.assertEqual(6, len(result["streams"]))

    @patch("tap_marketo.discover.check_stream_access", return_value=False)
    @patch("tap_marketo.discover.discover_catalog")
    @patch("tap_marketo.discover.discover_activities")
    @patch("tap_marketo.discover.discover_leads")
    def test_discover_raises_when_no_stream_access(self, mock_leads, mock_activities, mock_catalog, _mock_access):
        """Verifies discover() raises forbidden error when all streams are inaccessible."""
        mock_leads.return_value = None
        mock_activities.return_value = None
        mock_catalog.return_value = None

        with self.assertRaises(MarketoForbiddenError):
            discover(MagicMock())

    def test_discover_activities_calls_client(self):
        """Ensures activity type discovery requests and maps client results."""
        client = MagicMock()
        client.request.return_value = {
            "result": [
                {"id": 1, "name": "Visit Webpage", "attributes": []},
            ]
        }

        streams = discover_activities(client)

        self.assertEqual(1, len(streams))
        self.assertEqual("activities_visit_webpage", streams[0]["tap_stream_id"])

    def test_discover_leads_builds_metadata(self):
        """Ensures lead field discovery builds schema properties and metadata."""
        client = MagicMock()
        client.request.return_value = {
            "result": [
                {"displayName": "id", "dataType": "integer", "rest": {"name": "id"}},
                {"displayName": "updatedAt", "dataType": "datetime", "rest": {"name": "updatedAt"}},
                {"displayName": "no-rest", "dataType": "string"},
            ]
        }

        stream = discover_leads(client)

        self.assertEqual("leads", stream["tap_stream_id"])
        self.assertIn("id", stream["schema"]["properties"])
        self.assertIn("updatedAt", stream["schema"]["properties"])

    def test_discover_catalog_includes_metadata(self):
        """Verifies static catalog discovery includes key properties and root metadata."""
        stream = discover_catalog("campaigns", frozenset(["id", "createdAt", "updatedAt"]))

        self.assertEqual("campaigns", stream["tap_stream_id"])
        self.assertEqual(["id"], stream["key_properties"])
        self.assertTrue(any(m["breadcrumb"] == () for m in stream["metadata"]))

    @patch("tap_marketo.discover.singer.log_warning")
    def test_discover_activities_forbidden_returns_none(self, log_warning):
        """Verifies activity type discover excludes unauthorized stream on forbidden error."""
        client = MagicMock()
        client.request.side_effect = MarketoForbiddenError("forbidden")

        result = discover_activities(client)

        self.assertIsNone(result)
        log_warning.assert_called_once()

    @patch("tap_marketo.discover.singer.log_warning")
    def test_discover_leads_forbidden_returns_none(self, log_warning):
        """Verifies leads discover excludes unauthorized stream on forbidden error."""
        client = MagicMock()
        client.request.side_effect = MarketoForbiddenError("forbidden")

        result = discover_leads(client)

        self.assertIsNone(result)
        log_warning.assert_called_once()

    @patch("tap_marketo.discover.open", create=True)
    @patch("tap_marketo.discover.json.load")
    def test_discover_catalog_supports_wrapped_schema_shape(self, mock_load, _mock_open):
        """Verifies discover_catalog handles wrapped schema files with explicit tap_stream_id."""
        mock_load.return_value = {
            "tap_stream_id": "custom_campaigns",
            "key_properties": ["id"],
            "schema": {
                "type": "object",
                "properties": {
                    "id": {"type": ["integer", "null"]},
                    "createdAt": {"type": ["string", "null"]},
                    "ignored": {"type": ["string", "null"]},
                },
            },
        }

        stream = discover_catalog(
            "campaigns",
            automatic_inclusion=frozenset(["id", "createdAt"]),
            unsupported=frozenset(["ignored"]),
        )

        self.assertEqual("custom_campaigns", stream["tap_stream_id"])
        self.assertEqual(["id"], stream["key_properties"])

    @patch("tap_marketo.discover.open", create=True)
    @patch("tap_marketo.discover.json.load")
    def test_discover_catalog_supports_plain_schema_shape(self, mock_load, _mock_open):
        """Verifies discover_catalog handles plain schema files without wrapper keys."""
        mock_load.return_value = {
            "type": "object",
            "properties": {
                "id": {"type": ["integer", "null"]},
                "updatedAt": {"type": ["string", "null"], "format": "date-time"},
            },
        }

        stream = discover_catalog(
            "campaigns",
            automatic_inclusion=frozenset(["id", "updatedAt"]),
        )

        self.assertEqual("campaigns", stream["tap_stream_id"])
        self.assertEqual(["id"], stream["key_properties"])

    @patch("tap_marketo.discover.check_stream_access", return_value=False)
    def test_discover_catalog_returns_none_when_access_denied(self, _mock_access):
        """Verifies discover_catalog excludes streams when access check fails."""
        stream = discover_catalog("campaigns", CAMPAIGNS_AUTOMATIC_INCLUSION, client=MagicMock())
        self.assertIsNone(stream)

    def test_get_probe_key_for_activity_and_regular_streams(self):
        """Verifies activity sub-streams map to activity_types probe key."""
        self.assertEqual("activity_types", _get_probe_key("activities_open_email"))
        self.assertEqual("leads", _get_probe_key("leads"))

    @patch("tap_marketo.discover.singer.log_warning")
    def test_check_stream_access_returns_false_on_forbidden(self, log_warning):
        """Verifies check_stream_access returns False and logs on forbidden."""
        client = MagicMock()
        client.request.side_effect = MarketoForbiddenError("forbidden")

        result = check_stream_access(client, "campaigns")

        self.assertFalse(result)
        log_warning.assert_called_once()

    def test_check_stream_access_returns_true_for_unknown_stream(self):
        """Verifies unknown streams are treated as accessible."""
        client = MagicMock()
        self.assertTrue(check_stream_access(client, "unknown_stream"))
        client.request.assert_not_called()

    def test_check_stream_access_activity_stream_uses_activity_probe(self):
        """Verifies activity sub-stream access check uses shared activity_types probe endpoint."""
        client = MagicMock()
        client.request.return_value = {"result": []}

        result = check_stream_access(client, "activities_click_email")

        self.assertTrue(result)
        _, endpoint = client.request.call_args[0][0], client.request.call_args[0][1]
        self.assertEqual("rest/v1/activities/types.json", endpoint)

    @patch("tap_marketo.discover.discover_catalog")
    @patch("tap_marketo.discover.discover_activities")
    @patch("tap_marketo.discover.discover_leads")
    def test_discover_tracks_activity_types_inaccessible(self, mock_leads, mock_activities, mock_catalog):
        """Verifies discover records activity_types as inaccessible when subtype catalog is denied."""
        mock_leads.return_value = {"tap_stream_id": "leads"}
        mock_activities.return_value = [{"tap_stream_id": "activities_visit_webpage"}]
        mock_catalog.side_effect = [
            None,
            {"tap_stream_id": "campaigns"},
            {"tap_stream_id": "lists"},
            {"tap_stream_id": "programs"},
        ]

        result = discover(MagicMock())

        self.assertIn("streams", result)
        stream_names = [stream["tap_stream_id"] for stream in result["streams"]]
        self.assertNotIn("activity_types", stream_names)
        self.assertNotIn("activities_visit_webpage", stream_names)


if __name__ == "__main__":
    unittest.main()
