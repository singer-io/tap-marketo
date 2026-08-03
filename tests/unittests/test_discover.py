"""Unit tests for discover orchestration and catalog-building entry points."""

import unittest
from unittest.mock import MagicMock, patch

from tap_marketo.discover import discover, discover_catalog, discover_activities, discover_leads


class TestDiscover(unittest.TestCase):
    """Verifies discovery output shape and delegation behavior."""

    @patch("tap_marketo.discover.json.dump")
    @patch("tap_marketo.discover.discover_catalog")
    @patch("tap_marketo.discover.discover_activities")
    @patch("tap_marketo.discover.discover_leads")
    def test_discover_writes_catalog(self, mock_leads, mock_activities, mock_catalog, mock_dump):
        mock_leads.return_value = {"tap_stream_id": "leads"}
        mock_activities.return_value = [{"tap_stream_id": "activities_visit_webpage"}]
        mock_catalog.side_effect = [
            {"tap_stream_id": "activity_types"},
            {"tap_stream_id": "campaigns"},
            {"tap_stream_id": "lists"},
            {"tap_stream_id": "programs"},
        ]

        result = discover(MagicMock())

        self.assertIsNone(result)
        mock_dump.assert_called_once()
        payload = mock_dump.call_args[0][0]
        self.assertIn("streams", payload)
        self.assertEqual(6, len(payload["streams"]))

    def test_discover_activities_calls_client(self):
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
        stream = discover_catalog("campaigns", frozenset(["id", "createdAt", "updatedAt"]))

        self.assertEqual("campaigns", stream["tap_stream_id"])
        self.assertEqual(["id"], stream["key_properties"])
        self.assertTrue(any(m["breadcrumb"] == () for m in stream["metadata"]))


if __name__ == "__main__":
    unittest.main()
