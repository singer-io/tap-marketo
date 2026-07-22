import unittest
from unittest.mock import MagicMock, patch, call

from tap_marketo.client import ApiException, MarketoForbiddenError
from tap_marketo.discover import (
    check_stream_access,
    _apply_access_checks,
    discover,
    STREAM_PROBE_ENDPOINTS,
)


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _make_stream(tap_stream_id):
    replication_key = "activityDate" if tap_stream_id.startswith("activities_") else (
        "updatedAt" if tap_stream_id in ["leads", "campaigns", "lists", "programs"] else None)
    replication_method = "INCREMENTAL" if replication_key else "FULL_TABLE"
    parent_stream = "activity_types" if tap_stream_id.startswith("activities_") else None
    return {
        "tap_stream_id": tap_stream_id,
        "stream": tap_stream_id,
        "schema": {},
        "metadata": [],
        "replication_method": replication_method,
        "replication_key": replication_key,
        "parent_stream": parent_stream,
    }


def _mock_client(side_effect=None):
    client = MagicMock()
    if side_effect is not None:
        client.request.side_effect = side_effect
    return client


# ---------------------------------------------------------------------------
# check_stream_access
# ---------------------------------------------------------------------------

class TestCheckStreamAccess(unittest.TestCase):

    def test_returns_true_when_accessible(self):
        client = _mock_client()
        self.assertTrue(check_stream_access(client, "leads"))
        client.request.assert_called_once()

    def test_returns_false_on_forbidden(self):
        client = _mock_client(side_effect=MarketoForbiddenError("403"))
        self.assertFalse(check_stream_access(client, "leads"))

    def test_non_forbidden_exceptions_propagate(self):
        client = _mock_client(side_effect=ApiException("500"))
        with self.assertRaises(ApiException):
            check_stream_access(client, "leads")

    def test_activity_substream_delegates_to_activity_types_probe(self):
        """activities_* streams must probe the activity_types endpoint, not their own."""
        client = _mock_client()
        check_stream_access(client, "activities_visit_webpage")
        call_args = client.request.call_args
        # Second positional arg is the endpoint URL
        endpoint_used = call_args[0][1]
        expected_endpoint = STREAM_PROBE_ENDPOINTS["activity_types"][1]
        self.assertEqual(endpoint_used, expected_endpoint)

    def test_unknown_stream_returns_true(self):
        """Streams not in STREAM_PROBE_ENDPOINTS should be assumed accessible."""
        client = _mock_client()
        result = check_stream_access(client, "some_unknown_stream")
        self.assertTrue(result)
        client.request.assert_not_called()

    def test_all_known_streams_have_probe_endpoints(self):
        known_streams = ["leads", "activity_types", "campaigns", "lists", "programs"]
        for s in known_streams:
            self.assertIn(s, STREAM_PROBE_ENDPOINTS,
                          msg=f"Stream '{s}' missing from STREAM_PROBE_ENDPOINTS")


# ---------------------------------------------------------------------------
# _apply_access_checks
# ---------------------------------------------------------------------------

class TestApplyAccessChecks(unittest.TestCase):

    @patch("tap_marketo.discover.check_stream_access", return_value=True)
    def test_all_accessible_returns_all_streams(self, _mock_check):
        streams = [_make_stream("leads"), _make_stream("campaigns")]
        result = _apply_access_checks(MagicMock(), streams)
        self.assertEqual(len(result), 2)

    @patch("tap_marketo.discover.check_stream_access")
    def test_inaccessible_stream_excluded(self, mock_check):
        mock_check.side_effect = lambda client, name: name != "campaigns"
        streams = [_make_stream("leads"), _make_stream("campaigns")]
        result = _apply_access_checks(MagicMock(), streams)
        ids = [s["tap_stream_id"] for s in result]
        self.assertIn("leads", ids)
        self.assertNotIn("campaigns", ids)

    @patch("tap_marketo.discover.check_stream_access", return_value=False)
    def test_all_inaccessible_raises_forbidden(self, _mock_check):
        streams = [_make_stream("leads"), _make_stream("campaigns")]
        with self.assertRaises(MarketoForbiddenError) as ctx:
            _apply_access_checks(MagicMock(), streams)
        self.assertIn("HTTP-error-code: 403", str(ctx.exception))

    @patch("tap_marketo.discover.check_stream_access")
    def test_activity_substreams_share_single_probe(self, mock_check):
        """Multiple activities_* streams must only trigger one probe for activity_types."""
        mock_check.return_value = True
        streams = [
            _make_stream("activities_visit_webpage"),
            _make_stream("activities_fill_out_form"),
            _make_stream("activities_click_link"),
        ]
        _apply_access_checks(MagicMock(), streams)
        # check_stream_access should be called once for "activity_types", not 3 times
        probe_calls = [c[0][1] for c in mock_check.call_args_list]
        self.assertEqual(probe_calls.count("activity_types"), 1)

    @patch("tap_marketo.discover.check_stream_access")
    def test_inaccessible_activity_types_excludes_all_substreams(self, mock_check):
        mock_check.side_effect = lambda client, name: name != "activity_types"
        streams = [
            _make_stream("leads"),
            _make_stream("activity_types"),
            _make_stream("activities_visit_webpage"),
            _make_stream("activities_fill_out_form"),
        ]
        result = _apply_access_checks(MagicMock(), streams)
        ids = [s["tap_stream_id"] for s in result]
        self.assertIn("leads", ids)
        self.assertNotIn("activity_types", ids)
        self.assertNotIn("activities_visit_webpage", ids)
        self.assertNotIn("activities_fill_out_form", ids)

    @patch("tap_marketo.discover.check_stream_access")
    def test_parent_inaccessible_marks_child_in_inaccessible_logger(self, mock_check):
        """If a parent stream is inaccessible, child streams should be excluded and
        included in the same inaccessible log list without probing child endpoint."""
        mock_check.side_effect = lambda client, name: name != "activity_types"
        streams = [
            _make_stream("leads"),
            _make_stream("activity_types"),
            _make_stream("activities_visit_webpage"),
        ]
        with patch("tap_marketo.discover.singer.log_warning") as mock_warning:
            result = _apply_access_checks(MagicMock(), streams)

        ids = [s["tap_stream_id"] for s in result]
        self.assertIn("leads", ids)
        self.assertNotIn("activity_types", ids)
        self.assertNotIn("activities_visit_webpage", ids)
        mock_warning.assert_called_once()
        warning_msg = mock_warning.call_args[0][0]
        warning_streams = mock_warning.call_args[0][1]
        self.assertIn("No 'read' access to stream(s): %s. Excluded from catalog.", warning_msg)
        self.assertIn("activity_types", warning_streams)
        self.assertIn("activities_visit_webpage", warning_streams)


# ---------------------------------------------------------------------------
# discover()
# ---------------------------------------------------------------------------

class TestDiscover(unittest.TestCase):

    @patch("tap_marketo.discover._apply_access_checks", side_effect=lambda client, s: s)
    @patch("tap_marketo.discover.discover_activities", return_value=[_make_stream("activities_visit_webpage")])
    @patch("tap_marketo.discover.discover_leads", return_value=_make_stream("leads"))
    @patch("tap_marketo.discover.discover_catalog")
    def test_discover_returns_catalog_dict(self, mock_catalog, mock_leads, mock_acts, mock_checks):
        mock_catalog.return_value = _make_stream("campaigns")
        result = discover(MagicMock())
        self.assertIsInstance(result, dict)
        self.assertIn("streams", result)
        self.assertIsInstance(result["streams"], list)
        self.assertGreater(len(result["streams"]), 0)
        for stream in result["streams"]:
            self.assertIn("metadata", stream)
            self.assertIn("replication_method", stream)
            self.assertIn("replication_key", stream)
            self.assertIn("parent_stream", stream)

    @patch("tap_marketo.discover._apply_access_checks")
    @patch("tap_marketo.discover.discover_activities", return_value=[])
    @patch("tap_marketo.discover.discover_leads", return_value=_make_stream("leads"))
    @patch("tap_marketo.discover.discover_catalog", return_value=_make_stream("campaigns"))
    def test_discover_calls_access_checks(self, _c, _l, _a, mock_checks):
        mock_checks.side_effect = lambda client, s: s
        client = MagicMock()
        discover(client)
        mock_checks.assert_called_once()
        # client must be the first arg passed to _apply_access_checks
        self.assertIs(mock_checks.call_args[0][0], client)

    @patch("tap_marketo.discover._apply_access_checks",
           side_effect=MarketoForbiddenError("no access"))
    @patch("tap_marketo.discover.discover_activities", return_value=[])
    @patch("tap_marketo.discover.discover_leads", return_value=_make_stream("leads"))
    @patch("tap_marketo.discover.discover_catalog", return_value=_make_stream("campaigns"))
    def test_discover_propagates_forbidden_error(self, _c, _l, _a, _checks):
        with self.assertRaises(MarketoForbiddenError):
            discover(MagicMock())


if __name__ == "__main__":
    unittest.main()
