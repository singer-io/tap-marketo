from math import ceil

import pytest

pytest.importorskip("tap_tester")
from tap_tester import connections, runner

from base import MarketoBaseTest


class TestMarketoPagination(MarketoBaseTest):
    @staticmethod
    def name():
        return "tap_tester_marketo_pagination_test"

    @staticmethod
    def pagination_streams():
        return {"campaigns", "lists", "programs"}

    def test_run(self):
        conn_id = connections.ensure_connection(self)

        found_catalogs = self.run_and_verify_check_mode(conn_id)
        discovered_streams = {catalog.get("stream_name") for catalog in found_catalogs}
        streams_to_test = self.pagination_streams().intersection(discovered_streams)
        self.assertGreater(len(streams_to_test), 0)

        test_catalogs = [
            catalog for catalog in found_catalogs if catalog.get("stream_name") in streams_to_test
        ]
        self.perform_and_verify_table_and_field_selection(
            conn_id, test_catalogs, select_all_fields=True
        )

        sync_record_count = self.run_and_verify_sync(conn_id, streams_to_test)
        sync_records = runner.get_records_from_target_output()

        for stream in streams_to_test:
            with self.subTest(stream=stream):
                record_count = sync_record_count.get(stream, 0)
                self.assertGreater(record_count, 0)

                stream_messages = sync_records.get(stream, {"messages": []}).get("messages", [])
                primary_keys = self.expected_primary_keys().get(stream)
                stream_page_size = self.expected_page_limits()[stream]

                records_primary_keys = [
                    tuple(message.get("data", {}).get(primary_key) for primary_key in primary_keys)
                    for message in stream_messages
                    if message.get("action") == "upsert"
                ]
                self.assertCountEqual(set(records_primary_keys), records_primary_keys)

                if record_count <= stream_page_size:
                    continue

                pages = []
                page_count = ceil(len(records_primary_keys) / stream_page_size)
                for page_index in range(page_count):
                    page_start = page_index * stream_page_size
                    page_end = (page_index + 1) * stream_page_size
                    pages.append(set(records_primary_keys[page_start:page_end]))

                for current_index, current_page in enumerate(pages):
                    for other_index, other_page in enumerate(pages):
                        if current_index == other_index:
                            continue
                        self.assertTrue(current_page.isdisjoint(other_page))
