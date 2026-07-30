import unittest
from unittest.mock import MagicMock, patch

from singer import metadata

from tap_marketo.discover import (
    build_stream_entry,
    discover_activities,
    discover_leads,
    get_activity_type_stream,
    get_schema_for_type,
    set_replication_metadata,
)
from tap_marketo.client import MarketoForbiddenError


class TestDiscoverCoreHelpers(unittest.TestCase):
    def test_build_stream_entry_sets_root_inclusion_and_parent(self):
        mdata = metadata.new()

        entry = build_stream_entry(
            tap_stream_id="activities_visit_webpage",
            key_properties=["marketoGUID"],
            schema={"type": "object", "properties": {}},
            mdata=mdata,
            parent_stream="activity_types",
        )

        mdata_map = metadata.to_map(entry["metadata"])
        self.assertEqual("available", metadata.get(mdata_map, (), "inclusion"))
        self.assertEqual(
            "activity_types",
            metadata.get(mdata_map, (), "parent-tap-stream-id"),
        )
        self.assertEqual("INCREMENTAL", entry["replication_method"])
        self.assertEqual("activityDate", entry["replication_key"])

    def test_get_schema_for_type_writes_expected_types(self):
        mdata = metadata.new()

        schema_dt, mdata = get_schema_for_type("datetime", ("properties", "dt"), mdata, null=False)
        schema_int, mdata = get_schema_for_type("integer", ("properties", "int"), mdata, null=True)
        schema_bool, mdata = get_schema_for_type("boolean", ("properties", "flag"), mdata, null=True)
        schema_array, mdata = get_schema_for_type("array", ("properties", "arr"), mdata, null=True)
        schema_unknown, mdata = get_schema_for_type("custom", ("properties", "x"), mdata, null=True)
        schema_number, mdata = get_schema_for_type("currency", ("properties", "amount"), mdata, null=True)

        self.assertEqual("date-time", schema_dt["format"])
        self.assertEqual(["integer", "null"], schema_int["type"])
        self.assertEqual(["boolean", "null"], schema_bool["type"])
        self.assertEqual("array", schema_array["type"][0])
        self.assertEqual(["string", "null"], schema_unknown["type"])
        self.assertEqual(["number", "null"], schema_number["type"])

    def test_set_replication_metadata_handles_str_list_and_none(self):
        mdata_map = metadata.new()

        with_single = set_replication_metadata(mdata_map, "updatedAt")
        self.assertEqual("INCREMENTAL", metadata.get(with_single, (), "forced-replication-method"))
        self.assertEqual(["updatedAt"], metadata.get(with_single, (), "valid-replication-keys"))

        with_none = set_replication_metadata(metadata.new(), None)
        self.assertEqual("FULL_TABLE", metadata.get(with_none, (), "forced-replication-method"))
        self.assertIsNone(metadata.get(with_none, (), "valid-replication-keys"))


class TestDiscoverCoreStreams(unittest.TestCase):
    def test_get_activity_type_stream_with_primary_and_attributes(self):
        activity = {
            "id": 101,
            "name": "Visit Webpage",
            "primaryAttribute": {"name": "Webpage ID", "dataType": "integer"},
            "attributes": [
                {"name": "Client IP Address", "dataType": "string"},
                {"name": "Query Parameters", "dataType": "string"},
            ],
        }

        stream = get_activity_type_stream(activity)

        self.assertEqual("activities_visit_webpage", stream["tap_stream_id"])
        self.assertEqual("INCREMENTAL", stream["replication_method"])
        self.assertEqual("activityDate", stream["replication_key"])
        self.assertIn("primary_attribute_name", stream["schema"]["properties"])
        self.assertIn("client_ip_address", stream["schema"]["properties"])
        self.assertIn("query_parameters", stream["schema"]["properties"])

    @patch("tap_marketo.discover.singer.log_warning")
    def test_discover_activities_forbidden_returns_none(self, _warn):
        client = MagicMock()
        client.request.side_effect = MarketoForbiddenError("403")

        self.assertIsNone(discover_activities(client))

    @patch("tap_marketo.discover.singer.log_warning")
    def test_discover_leads_forbidden_returns_none(self, _warn):
        client = MagicMock()
        client.request.side_effect = MarketoForbiddenError("403")

        self.assertIsNone(discover_leads(client))

    def test_discover_leads_skips_non_rest_and_builds_schema(self):
        client = MagicMock()
        client.request.return_value = {
            "result": [
                {"displayName": "No REST Field", "dataType": "string"},
                {"displayName": "Lead ID", "dataType": "integer", "rest": {"name": "id"}},
                {"displayName": "Updated At", "dataType": "datetime", "rest": {"name": "updatedAt"}},
                {"displayName": "Custom Text", "dataType": "string", "rest": {"name": "customText"}},
            ]
        }

        stream = discover_leads(client)
        self.assertEqual("leads", stream["tap_stream_id"])
        self.assertEqual("INCREMENTAL", stream["replication_method"])
        self.assertEqual("updatedAt", stream["replication_key"])
        self.assertIn("id", stream["schema"]["properties"])
        self.assertIn("updatedAt", stream["schema"]["properties"])
        self.assertIn("customText", stream["schema"]["properties"])

    @patch("tap_marketo.discover.get_activity_type_stream")
    def test_discover_activities_maps_results(self, mock_get_activity):
        mock_get_activity.side_effect = [
            {"tap_stream_id": "activities_a"},
            {"tap_stream_id": "activities_b"},
        ]
        client = MagicMock()
        client.request.return_value = {"result": [{"id": 1}, {"id": 2}]}

        streams = discover_activities(client)

        self.assertEqual(["activities_a", "activities_b"], [s["tap_stream_id"] for s in streams])
        self.assertEqual(2, mock_get_activity.call_count)

    @patch("tap_marketo.discover.singer.log_debug")
    @patch("tap_marketo.discover.get_schema_for_type")
    def test_discover_leads_handles_unsupported_schema(self, mock_get_schema, log_debug):
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

    @patch("tap_marketo.discover.json.load")
    @patch("tap_marketo.discover.open")
    def test_discover_catalog_uses_wrapped_schema_and_unsupported_inclusion(self, _open_file, mock_json_load):
        mock_json_load.return_value = {
            "tap_stream_id": "custom_campaigns",
            "schema": {
                "type": "object",
                "properties": {
                    "id": {"type": "integer"},
                    "attributes": {"type": "array"},
                    "name": {"type": "string"},
                },
            },
            "key_properties": ["id"],
        }

        from tap_marketo.discover import discover_catalog

        stream = discover_catalog(
            "campaigns",
            automatic_inclusion=frozenset(["id"]),
            unsupported=frozenset(["attributes"]),
        )

        mdata_map = metadata.to_map(stream["metadata"])
        self.assertEqual("custom_campaigns", stream["tap_stream_id"])
        self.assertEqual(["id"], stream["key_properties"])
        self.assertEqual("unsupported", metadata.get(mdata_map, ("properties", "attributes"), "inclusion"))


if __name__ == "__main__":
    unittest.main()
