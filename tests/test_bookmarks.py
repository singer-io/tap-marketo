import dateutil.parser
import pytest

pytest.importorskip("tap_tester")
from tap_tester import connections, menagerie, runner

from tests.base import MarketoBaseTest


class TestMarketoBookmarks(MarketoBaseTest):
    @staticmethod
    def name():
        return "tap_tester_marketo_bookmarks_test"

    def test_run(self):
        conn_id = connections.ensure_connection(self)

        found_catalogs = self.run_and_verify_check_mode(conn_id)
        discovered_streams = {catalog.get("stream_name") for catalog in found_catalogs}
        streams_to_test = self.expected_streams().intersection(discovered_streams)
        self.assertGreater(len(streams_to_test), 0)

        expected_replication_keys = self.expected_replication_keys()

        test_catalogs = [
            catalog for catalog in found_catalogs if catalog.get("stream_name") in streams_to_test
        ]
        self.perform_and_verify_table_and_field_selection(
            conn_id, test_catalogs, select_all_fields=True
        )

        first_sync_record_count = self.run_and_verify_sync(conn_id, streams_to_test)
        first_sync_records = runner.get_records_from_target_output()
        first_sync_bookmarks = menagerie.get_state(conn_id)

        new_state = {"bookmarks": dict()}
        simulated_states = self.calculated_states_by_stream(first_sync_bookmarks, streams_to_test)
        for stream, updated_state in simulated_states.items():
            new_state["bookmarks"][stream] = updated_state
        menagerie.set_state(conn_id, new_state)

        second_sync_record_count = self.run_and_verify_sync(conn_id, streams_to_test)
        second_sync_records = runner.get_records_from_target_output()
        second_sync_bookmarks = menagerie.get_state(conn_id)

        self.assertNotIn("currently_syncing", first_sync_bookmarks)
        self.assertNotIn("currently_syncing", second_sync_bookmarks)

        for stream in streams_to_test:
            with self.subTest(stream=stream):
                replication_key = next(iter(expected_replication_keys[stream]))

                first_sync_count = first_sync_record_count.get(stream, 0)
                second_sync_count = second_sync_record_count.get(stream, 0)
                self.assertGreater(second_sync_count, 0)

                first_sync_messages = [
                    record.get("data")
                    for record in first_sync_records.get(stream, {}).get("messages", [])
                    if record.get("action") == "upsert"
                ]
                second_sync_messages = [
                    record.get("data")
                    for record in second_sync_records.get(stream, {}).get("messages", [])
                    if record.get("action") == "upsert"
                ]

                first_bookmark_value = first_sync_bookmarks.get("bookmarks", {}).get(stream, {}).get(replication_key)
                second_bookmark_value = second_sync_bookmarks.get("bookmarks", {}).get(stream, {}).get(replication_key)

                self.assertIsNotNone(first_bookmark_value)
                self.assertIsNotNone(second_bookmark_value)

                first_bookmark_datetime = dateutil.parser.parse(first_bookmark_value)
                second_bookmark_datetime = dateutil.parser.parse(second_bookmark_value)

                self.assertLessEqual(first_bookmark_datetime, second_bookmark_datetime)

                simulated_bookmark = new_state["bookmarks"].get(stream, {}).get(replication_key)
                if simulated_bookmark:
                    simulated_bookmark_datetime = dateutil.parser.parse(simulated_bookmark)
                    for record in second_sync_messages:
                        replication_key_value = record.get(replication_key)
                        self.assertGreaterEqual(
                            dateutil.parser.parse(replication_key_value),
                            simulated_bookmark_datetime,
                        )

                for record in first_sync_messages:
                    replication_key_value = record.get(replication_key)
                    self.assertLessEqual(
                        dateutil.parser.parse(replication_key_value),
                        first_bookmark_datetime,
                    )

                for record in second_sync_messages:
                    replication_key_value = record.get(replication_key)
                    self.assertLessEqual(
                        dateutil.parser.parse(replication_key_value),
                        second_bookmark_datetime,
                    )

                self.assertLessEqual(second_sync_count, first_sync_count)
