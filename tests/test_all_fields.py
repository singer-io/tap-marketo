import pytest

pytest.importorskip("tap_tester")
from tap_tester import connections, menagerie, runner

from tests.base import MarketoBaseTest


class TestMarketoAllFields(MarketoBaseTest):
    @staticmethod
    def name():
        return "tap_tester_marketo_all_fields_test"

    def test_run(self):
        conn_id = connections.ensure_connection(self)

        found_catalogs = self.run_and_verify_check_mode(conn_id)
        discovered_streams = {catalog.get("stream_name") for catalog in found_catalogs}
        streams_to_test = self.expected_streams().intersection(discovered_streams)
        self.assertGreater(len(streams_to_test), 0)

        test_catalogs = [
            catalog for catalog in found_catalogs if catalog.get("stream_name") in streams_to_test
        ]
        self.perform_and_verify_table_and_field_selection(
            conn_id, test_catalogs, select_all_fields=True
        )

        sync_record_count = self.run_and_verify_sync(conn_id, streams_to_test)
        sync_records = runner.get_records_from_target_output()

        self.assertSetEqual(streams_to_test, set(sync_records.keys()))

        catalog_all_fields = {}
        for catalog in test_catalogs:
            stream_id = catalog["stream_id"]
            stream_name = catalog["stream_name"]
            catalog_entry = menagerie.get_annotated_schema(conn_id, stream_id)
            fields_from_field_metadata = [
                metadata_entry["breadcrumb"][1]
                for metadata_entry in catalog_entry["metadata"]
                if metadata_entry["breadcrumb"] != []
            ]
            catalog_all_fields[stream_name] = set(fields_from_field_metadata)

        for stream in streams_to_test:
            with self.subTest(stream=stream):
                expected_automatic_fields = self.expected_automatic_fields().get(stream)
                expected_all_fields = catalog_all_fields[stream]

                self.assertGreater(sync_record_count.get(stream, 0), 0)

                messages = sync_records.get(stream, {"messages": []})
                actual_all_fields = set()
                all_record_fields = [
                    set(message["data"].keys())
                    for message in messages["messages"]
                    if message.get("action") == "upsert"
                ]
                for field_set in all_record_fields:
                    actual_all_fields.update(field_set)

                self.assertTrue(expected_automatic_fields.issubset(actual_all_fields))
                self.assertSetEqual(
                    expected_all_fields - self.MISSING_FIELDS.get(stream, set()),
                    actual_all_fields,
                )
