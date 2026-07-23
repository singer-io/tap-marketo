import unittest
from unittest.mock import MagicMock, patch

from tap_marketo.client import ApiException, MarketoForbiddenError
from tap_marketo.discover import (
    check_stream_access,
    discover,
    discover_catalog,
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


class TestDiscoverCatalogAccess(unittest.TestCase):

    @patch("tap_marketo.discover.check_stream_access", return_value=True)
    def test_discover_catalog_returns_stream_when_accessible(self, _mock_check):
        stream = discover_catalog("campaigns", frozenset(["id"]), client=MagicMock())
        self.assertIsNotNone(stream)
        self.assertEqual(stream["tap_stream_id"], "campaigns")

    @patch("tap_marketo.discover.check_stream_access", return_value=False)
    def test_discover_catalog_returns_none_when_inaccessible(self, _mock_check):
        stream = discover_catalog("campaigns", frozenset(["id"]), client=MagicMock())
        self.assertIsNone(stream)


# ---------------------------------------------------------------------------
# discover()
# ---------------------------------------------------------------------------

class TestDiscover(unittest.TestCase):

    @patch("tap_marketo.discover.discover_activities", return_value=[_make_stream("activities_visit_webpage")])
    @patch("tap_marketo.discover.discover_leads", return_value=_make_stream("leads"))
    @patch("tap_marketo.discover.discover_catalog")
    def test_discover_returns_catalog_dict(self, mock_catalog, mock_leads, mock_acts):
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

    @patch("tap_marketo.discover.discover_activities", return_value=None)
    @patch("tap_marketo.discover.discover_leads", return_value=None)
    @patch("tap_marketo.discover.discover_catalog", return_value=None)
    def test_discover_raises_forbidden_if_all_streams_excluded(self, _c, _l, _a):
        with self.assertRaises(MarketoForbiddenError):
            discover(MagicMock())


if __name__ == "__main__":
    unittest.main()
