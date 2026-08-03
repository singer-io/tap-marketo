import json

import pytest

pytest.importorskip("tap_tester")
from tap_tester import connections, runner

from base import MarketoBaseTest


class TestMarketoAutomaticFields(MarketoBaseTest):
    @staticmethod
    def name():
        return "tap_tester_marketo_automatic_fields_test"

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
            conn_id, test_catalogs, select_all_fields=False
        )

        record_count_by_stream = self.run_and_verify_sync(conn_id, streams_to_test)
        all_messages = runner.get_records_from_target_output()

        for stream in streams_to_test:
            with self.subTest(stream=stream):
                expected_primary_keys = self.expected_primary_keys()
                expected_keys = self.expected_automatic_fields().get(stream)

                self.assertGreater(record_count_by_stream.get(stream, 0), 0)

                stream_messages = all_messages.get(stream, {"messages": []})
                record_messages_keys = [
                    set(message["data"].keys())
                    for message in stream_messages["messages"]
                    if message.get("action") == "upsert"
                ]

                for actual_keys in record_messages_keys:
                    self.assertSetEqual(expected_keys, actual_keys)

                records = [
                    message.get("data")
                    for message in stream_messages.get("messages", [])
                    if message.get("action") == "upsert"
                ]
                records_pks_list = [
                    tuple(message.get(primary_key) for primary_key in expected_primary_keys[stream])
                    for message in [json.loads(text_value) for text_value in {json.dumps(data_value) for data_value in records}]
                ]
                records_pks_set = set(records_pks_list)
                self.assertEqual(len(records), len(records_pks_set))
