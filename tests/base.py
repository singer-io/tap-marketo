import os
import unittest
from datetime import datetime, timedelta

import dateutil.parser
import pytest

pytest.importorskip("tap_tester")
from tap_tester import LOGGER, connections, menagerie, runner


class MarketoBaseTest(unittest.TestCase):
    AUTOMATIC_FIELDS = "automatic"
    REPLICATION_KEYS = "valid-replication-keys"
    PRIMARY_KEYS = "table-key-properties"
    REPLICATION_METHOD = "forced-replication-method"
    INCREMENTAL = "INCREMENTAL"
    FULL_TABLE = "FULL_TABLE"
    EXPECTED_PAGE_SIZE = "expected-page-size"
    START_DATE_FORMAT = "%Y-%m-%dT%H:%M:%SZ"
    BOOKMARK_DATE_FORMAT = "%Y-%m-%dT%H:%M:%S%z"

    MISSING_FIELDS = {}

    @staticmethod
    def tap_name():
        return "tap-marketo"

    @staticmethod
    def get_type():
        return "platform.marketobulk"

    def get_properties(self, original=True):
        return {
            "start_date": os.getenv("TAP_MARKETO_START_DATE", "2023-01-01T00:00:00Z"),
            "endpoint": os.getenv("TAP_MARKETO_ENDPOINT"),
            "client_id": os.getenv("TAP_MARKETO_CLIENT_ID"),
        }

    @staticmethod
    def get_credentials():
        return {
            "client_secret": os.getenv("TAP_MARKETO_CLIENT_SECRET"),
        }

    @staticmethod
    def required_environment_variables():
        return {"TAP_MARKETO_ENDPOINT", "TAP_MARKETO_CLIENT_ID", "TAP_MARKETO_CLIENT_SECRET"}

    @classmethod
    def expected_metadata(cls):
        return {
            "leads": {
                cls.PRIMARY_KEYS: {"id"},
                cls.REPLICATION_METHOD: cls.INCREMENTAL,
                cls.REPLICATION_KEYS: {"updatedAt"},
            },
            "campaigns": {
                cls.PRIMARY_KEYS: {"id"},
                cls.REPLICATION_METHOD: cls.INCREMENTAL,
                cls.REPLICATION_KEYS: {"updatedAt"},
                cls.EXPECTED_PAGE_SIZE: 300,
            },
            "lists": {
                cls.PRIMARY_KEYS: {"id"},
                cls.REPLICATION_METHOD: cls.INCREMENTAL,
                cls.REPLICATION_KEYS: {"updatedAt"},
                cls.EXPECTED_PAGE_SIZE: 300,
            },
            "programs": {
                cls.PRIMARY_KEYS: {"id"},
                cls.REPLICATION_METHOD: cls.INCREMENTAL,
                cls.REPLICATION_KEYS: {"updatedAt"},
                cls.EXPECTED_PAGE_SIZE: 200,
            },
        }

    @classmethod
    def expected_streams(cls):
        return set(cls.expected_metadata().keys())

    @classmethod
    def expected_primary_keys(cls):
        return {
            stream: properties.get(cls.PRIMARY_KEYS, set())
            for stream, properties in cls.expected_metadata().items()
        }

    @classmethod
    def expected_replication_keys(cls):
        return {
            stream: properties.get(cls.REPLICATION_KEYS, set())
            for stream, properties in cls.expected_metadata().items()
        }

    @classmethod
    def expected_automatic_fields(cls):
        return {
            stream: properties.get(cls.PRIMARY_KEYS, set()) | properties.get(cls.REPLICATION_KEYS, set())
            for stream, properties in cls.expected_metadata().items()
        }

    @classmethod
    def expected_replication_method(cls):
        return {
            stream: properties.get(cls.REPLICATION_METHOD)
            for stream, properties in cls.expected_metadata().items()
        }

    @classmethod
    def expected_page_limits(cls):
        return {
            stream: properties.get(cls.EXPECTED_PAGE_SIZE)
            for stream, properties in cls.expected_metadata().items()
            if properties.get(cls.EXPECTED_PAGE_SIZE)
        }

    def setUp(self):
        missing_envs = [env_var for env_var in self.required_environment_variables() if not os.getenv(env_var)]
        if missing_envs:
            raise Exception(f"Missing environment variables, please set {missing_envs}.")

    def run_and_verify_check_mode(self, conn_id):
        check_job_name = runner.run_check_mode(self, conn_id)

        exit_status = menagerie.get_exit_status(conn_id, check_job_name)
        menagerie.verify_check_exit_status(self, exit_status, check_job_name)

        found_catalogs = menagerie.get_catalogs(conn_id)
        self.assertGreater(len(found_catalogs), 0, msg=f"unable to locate schemas for connection {conn_id}")

        found_catalog_names = {found_catalog["stream_name"] for found_catalog in found_catalogs}
        expected_intersection = self.expected_streams().intersection(found_catalog_names)
        self.assertGreater(len(expected_intersection), 0, msg="none of the expected core streams were discovered")
        LOGGER.info("discovered schemas include expected core streams")

        return found_catalogs

    def run_and_verify_sync(self, conn_id, expected_streams):
        sync_job_name = runner.run_sync_mode(self, conn_id)

        exit_status = menagerie.get_exit_status(conn_id, sync_job_name)
        menagerie.verify_sync_exit_status(self, exit_status, sync_job_name)

        sync_record_count = runner.examine_target_output_file(
            self, conn_id, expected_streams, self.expected_primary_keys()
        )
        self.assertGreater(
            sum(sync_record_count.values()),
            0,
            msg=f"failed to replicate any data: {sync_record_count}",
        )
        LOGGER.info("total replicated row count: %s", sum(sync_record_count.values()))

        return sync_record_count

    def perform_and_verify_table_and_field_selection(self, conn_id, test_catalogs, select_all_fields=True):
        self.select_all_streams_and_fields(conn_id=conn_id, catalogs=test_catalogs, select_all_fields=select_all_fields)

        catalogs = menagerie.get_catalogs(conn_id)
        expected_selected = [catalog.get("stream_name") for catalog in test_catalogs]

        for catalog in catalogs:
            catalog_entry = menagerie.get_annotated_schema(conn_id, catalog["stream_id"])
            selected = catalog_entry.get("annotated-schema", {}).get("selected")
            if catalog["stream_name"] not in expected_selected:
                self.assertFalse(selected, msg="Stream selected, but not testable.")
                continue

            self.assertTrue(selected, msg="Stream not selected.")

            if select_all_fields:
                for _, field_props in catalog_entry.get("annotated-schema", {}).get("properties", {}).items():
                    self.assertTrue(field_props.get("selected"), msg="Field not selected.")
            else:
                expected_automatic_fields = self.expected_automatic_fields().get(catalog["stream_name"])
                selected_fields = self.get_selected_fields_from_metadata(catalog_entry["metadata"])
                self.assertEqual(expected_automatic_fields, selected_fields)

    @staticmethod
    def get_selected_fields_from_metadata(metadata_entries):
        selected_fields = set()
        for field in metadata_entries:
            is_field_metadata = len(field["breadcrumb"]) > 1
            inclusion = field.get("metadata", {}).get("inclusion")
            selected = field.get("metadata", {}).get("selected")
            if is_field_metadata and (selected is True or inclusion == "automatic"):
                selected_fields.add(field["breadcrumb"][1])
        return selected_fields

    @staticmethod
    def select_all_streams_and_fields(conn_id, catalogs, select_all_fields=True):
        for catalog in catalogs:
            schema = menagerie.get_annotated_schema(conn_id, catalog["stream_id"])
            non_selected_properties = []
            if not select_all_fields:
                non_selected_properties = schema.get("annotated-schema", {}).get("properties", {}).keys()

            connections.select_catalog_and_fields_via_metadata(
                conn_id,
                catalog,
                schema,
                [],
                non_selected_properties,
            )

    def calculated_states_by_stream(self, current_state, streams_to_test):
        stream_to_calculated_state = {}
        replication_keys = self.expected_replication_keys()

        for stream in streams_to_test:
            stream_bookmarks = current_state.get("bookmarks", {}).get(stream, {})
            replication_key = next(iter(replication_keys[stream]))
            bookmark_value = stream_bookmarks.get(replication_key)
            if bookmark_value is None:
                continue

            bookmark_datetime = dateutil.parser.parse(bookmark_value)
            calculated_datetime = bookmark_datetime - timedelta(days=2)
            stream_to_calculated_state[stream] = {
                replication_key: calculated_datetime.isoformat()
            }

        return stream_to_calculated_state

    @staticmethod
    def assert_is_date_format(value, str_format):
        try:
            datetime.strptime(value, str_format)
        except ValueError as error:
            raise AssertionError(
                f"Value does not conform to expected format: {str_format}"
            ) from error
