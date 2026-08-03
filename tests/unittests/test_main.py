"""Unit tests for tap_marketo main entrypoint and state validation wiring."""

import unittest
import runpy
from unittest.mock import MagicMock, patch

import tap_marketo


class TestMainModule(unittest.TestCase):
    """Ensures CLI/main orchestration calls the expected internal flows."""

    @patch("tap_marketo.singer.write_state")
    def test_validate_state_sets_missing_bookmark(self, _write_state):
        config = {"start_date": "2024-01-01T00:00:00Z"}
        catalog = {
            "streams": [
                {
                    "tap_stream_id": "leads",
                    "metadata": [{"breadcrumb": [], "metadata": {"selected": True}}],
                },
                {
                    "tap_stream_id": "activity_types",
                    "metadata": [{"breadcrumb": [], "metadata": {"selected": True}}],
                },
            ]
        }
        state = {"bookmarks": {}}

        updated_state = tap_marketo.validate_state(config, catalog, state)

        self.assertEqual(
            "2024-01-01T00:00:00Z",
            updated_state["bookmarks"]["leads"]["updatedAt"],
        )
        self.assertNotIn("activity_types", updated_state.get("bookmarks", {}))

    @patch("tap_marketo.singer.write_state")
    def test_validate_state_unsets_currently_syncing_for_deselected_stream(self, _write_state):
        config = {"start_date": "2024-01-01T00:00:00Z"}
        catalog = {
            "streams": [
                {
                    "tap_stream_id": "leads",
                    "metadata": [{"breadcrumb": [], "metadata": {"selected": False}}],
                }
            ]
        }
        state = {"currently_syncing": "leads", "bookmarks": {}}

        updated_state = tap_marketo.validate_state(config, catalog, state)

        self.assertIsNone(updated_state.get("currently_syncing"))

    @patch("tap_marketo.discover", return_value={"streams": []})
    @patch("tap_marketo.Client")
    def test__main_discover_mode(self, mock_client, mock_discover):
        config = {"endpoint": "123-ABC-456", "client_id": "id", "client_secret": "secret", "start_date": "2024-01-01T00:00:00Z"}

        tap_marketo._main(config, None, {}, discover_mode=True)

        mock_client.assert_called_once_with(**config)
        mock_discover.assert_called_once()

    @patch("tap_marketo.sync")
    @patch("tap_marketo.validate_state", return_value={"bookmarks": {}})
    @patch("tap_marketo.Client")
    def test__main_sync_mode_calls_validate_and_sync(self, mock_client, mock_validate_state, mock_sync):
        config = {"endpoint": "123-ABC-456", "client_id": "id", "client_secret": "secret", "start_date": "2024-01-01T00:00:00Z"}
        properties = {"streams": []}
        state = {"bookmarks": {}}

        tap_marketo._main(config, properties, state, discover_mode=False)

        mock_validate_state.assert_called_once_with(config, properties, state)
        mock_sync.assert_called_once()

    @patch("tap_marketo.singer.log_critical")
    @patch("tap_marketo._main", side_effect=RuntimeError("boom"))
    @patch("tap_marketo.singer.utils.parse_args")
    def test_main_logs_and_reraises(self, mock_parse_args, _mock_main, mock_log_critical):
        args = MagicMock()
        args.config = {"endpoint": "123-ABC-456", "client_id": "id", "client_secret": "secret", "start_date": "2024-01-01T00:00:00Z"}
        args.properties = None
        args.catalog = {"streams": []}
        args.state = {}
        args.discover = False
        mock_parse_args.return_value = args

        with self.assertRaises(RuntimeError):
            tap_marketo.main()

        mock_log_critical.assert_called_once()

    @patch("tap_marketo.client.Client")
    @patch("singer.utils.parse_args")
    def test_module_guard_executes_main(self, mock_parse_args, mock_client):
        args = MagicMock()
        args.config = {
            "endpoint": "123-ABC-456",
            "client_id": "id",
            "client_secret": "secret",
            "start_date": "2024-01-01T00:00:00Z",
        }
        args.properties = None
        args.catalog = None
        args.state = {}
        args.discover = False
        mock_parse_args.return_value = args

        runpy.run_path(tap_marketo.__file__, run_name="__main__")

        mock_client.assert_called_once_with(**args.config)
