"""Unit tests for discovery helper functions and stream-specific builders."""

import unittest
from unittest.mock import MagicMock, patch

from singer import metadata

from tap_marketo.discover import (
    get_activity_type_stream,
    get_schema_for_type,
    set_replication_metadata,
    discover_leads,
    discover_activities,
)


class TestDiscoverCoreHelpers(unittest.TestCase):
    """Covers schema typing and replication metadata helper behavior."""

    def test_get_schema_for_type_variants(self):
        """Verifies schema/type mapping across datetime, scalar, array, and fallback types."""
        mdata = metadata.new()

        dt_schema, mdata = get_schema_for_type("datetime", ("properties", "dt"), mdata, null=False)
        int_schema, mdata = get_schema_for_type("integer", ("properties", "num"), mdata, null=True)
        bool_schema, mdata = get_schema_for_type("boolean", ("properties", "flag"), mdata, null=True)
        number_schema, mdata = get_schema_for_type("currency", ("properties", "amount"), mdata, null=True)
        default_schema, mdata = get_schema_for_type("custom_unknown", ("properties", "fallback"), mdata, null=True)
        arr_schema, _ = get_schema_for_type("array", ("properties", "arr"), mdata, null=True)

        self.assertEqual("date-time", dt_schema["format"])
        self.assertEqual(["integer", "null"], int_schema["type"])
        self.assertEqual(["boolean", "null"], bool_schema["type"])
        self.assertEqual(["number", "null"], number_schema["type"])
        self.assertEqual(["string", "null"], default_schema["type"])
        self.assertEqual("array", arr_schema["type"][0])

    def test_set_replication_metadata_full_table_and_incremental(self):
        """Verifies replication metadata differs correctly for full-table vs incremental streams."""
        full = set_replication_metadata(metadata.new(), None)
        full_map = metadata.to_map(metadata.to_list(full))
        self.assertEqual("FULL_TABLE", full_map[()]["forced-replication-method"])

        incremental = set_replication_metadata(metadata.new(), "updatedAt")
        inc_map = metadata.to_map(metadata.to_list(incremental))
        self.assertEqual("INCREMENTAL", inc_map[()]["forced-replication-method"])
        self.assertEqual("updatedAt", inc_map[()]["valid-replication-keys"])

    def test_get_activity_type_stream(self):
        """Ensures activity type rows are converted into normalized stream schemas."""
        activity = {
            "id": 101,
            "name": "Visit Webpage",
            "primaryAttribute": {"name": "Webpage ID", "dataType": "integer"},
            "attributes": [{"name": "Client IP Address", "dataType": "string"}],
        }

        stream = get_activity_type_stream(activity)

        self.assertEqual("activities_visit_webpage", stream["tap_stream_id"])
        self.assertIn("client_ip_address", stream["schema"]["properties"])
        self.assertIn("primary_attribute_name", stream["schema"]["properties"])

    def test_get_activity_type_stream_replication_method(self):
        """Verifies activity streams get INCREMENTAL replication with activityDate as replication key."""
        activity = {
            "id": 102,
            "name": "Email Sent",
        }

        stream = get_activity_type_stream(activity)

        # Extract metadata map
        mdata_map = {tuple(m["breadcrumb"]): m["metadata"] for m in stream["metadata"]}
        root_metadata = mdata_map.get((), {})

        # Verify INCREMENTAL replication method
        self.assertEqual("INCREMENTAL", root_metadata.get("forced-replication-method"))
        # Verify activityDate is the replication key
        self.assertEqual("activityDate", root_metadata.get("valid-replication-keys"))
        # Verify tap_stream_id follows naming convention
        self.assertEqual("activities_email_sent", stream["tap_stream_id"])


class TestDiscoverCoreStreams(unittest.TestCase):
    """Validates stream-level discovery transformations and edge cases."""

    @patch("tap_marketo.discover.get_activity_type_stream")
    def test_discover_activities_maps_rows(self, mock_get_activity):
        """Verifies discover_activities maps each API row through stream-builder helper."""
        mock_get_activity.side_effect = [{"tap_stream_id": "activities_a"}, {"tap_stream_id": "activities_b"}]
        client = MagicMock()
        client.request.return_value = {"result": [{"id": 1}, {"id": 2}]}

        streams = discover_activities(client)
        self.assertEqual(["activities_a", "activities_b"], [s["tap_stream_id"] for s in streams])

    @patch("tap_marketo.discover.singer.log_debug")
    @patch("tap_marketo.discover.get_schema_for_type")
    def test_discover_leads_handles_unsupported_schema(self, mock_get_schema, log_debug):
        """Ensures unsupported lead field types are skipped with an explanatory debug log."""
        mock_get_schema.return_value = (None, metadata.new())
        client = MagicMock()
        client.request.return_value = {
            "result": [
                {"displayName": "Field A", "dataType": "custom", "rest": {"name": "fieldA"}},
            ]
        }

        stream = discover_leads(client)

        self.assertEqual({}, stream["schema"]["properties"])
        log_debug.assert_called_with("Marketo type %s unsupported for leads.%s", "custom", "fieldA")


if __name__ == "__main__":
    unittest.main()
