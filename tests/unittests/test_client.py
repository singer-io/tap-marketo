"""Unit tests for tap_marketo client request, auth, and export helpers."""

import itertools
import logging
import unittest
import unittest.mock

import freezegun
import pendulum
import requests
import requests_mock

from tap_marketo.client import *


logging.disable(logging.CRITICAL)


class TestClient(unittest.TestCase):
    """Validates client authentication, quota handling, and request behavior."""

    def setUp(self):
        self.client = Client("123-ABC-789", "id", "secret")

    def test_extract_domain(self):
        """Verifies domain extraction and invalid domain error handling."""
        self.assertEqual("123-ABC-789", extract_domain("https://123-ABC-789.mktorest.com/rest"))
        with self.assertRaises(ValueError):
            extract_domain("notadomain")

    @freezegun.freeze_time("2017-01-01")
    def test_refresh_token(self):
        """Ensures successful token refresh stores token and adjusted expiry timestamp."""
        with requests_mock.Mocker(real_http=True) as mock:
            mock.register_uri("GET", self.client.get_url("identity/oauth/token"), json={"access_token": "token", "expires_in": 1800})
            self.client.refresh_token()

        expires = pendulum.datetime(2017, 1, 1).add(seconds=1800 - 15)
        self.assertEqual("token", self.client.access_token)
        self.assertEqual(expires, self.client.token_expires)

    def test_refresh_token_error_not_2xx(self):
        """Ensures non-2xx auth responses raise ApiException."""
        with requests_mock.Mocker(real_http=True) as mock:
            mock.register_uri("GET", self.client.get_url("identity/oauth/token"), status_code=404)
            with self.assertRaises(ApiException):
                self.client.refresh_token()

    def test_refresh_token_error_raises_exception(self):
        """Ensures error payloads in auth responses raise ApiException."""
        with requests_mock.Mocker(real_http=True) as mock:
            mock.register_uri("GET", self.client.get_url("identity/oauth/token"), json={"error": "oops"})
            with self.assertRaises(ApiException):
                self.client.refresh_token()

    def test_expired_token_refreshes(self):
        # make sure token looks expired
        self.client.token_expires = pendulum.utcnow().subtract(days=1)
        # make sure calls_today doesn't update
        self.client.calls_today = 1
        with requests_mock.Mocker(real_http=True) as mock:
            # the endpoitn we're going to call to make sure refresh_token gets called
            mock.register_uri("GET", self.client.get_url("what"), json={"success": True})
            # mock out the refresh_token endpoint
            mock.register_uri("GET", self.client.get_url("identity/oauth/token"), json={"access_token": "token", "expires_in": 1800})
            # make the request
            self.client.request("GET", "what")

        self.assertEqual("token", self.client.access_token)

    def test_update_calls_today(self):
        """Verifies daily usage totals are read and persisted to calls_today."""
        self.client.token_expires = pendulum.utcnow().add(days=1)
        with requests_mock.Mocker(real_http=True) as mock:
            mock.register_uri("GET", self.client.get_url("rest/v1/stats/usage.json"), json={"result": [{"total": 200}]})
            self.client.update_calls_today()

        self.assertEqual(200, self.client.calls_today)

    def test_calls_today_updates(self):
        # disable refresh_token being called
        self.client.token_expires = pendulum.utcnow().add(days=1)
        # sanity check - make sure we don't have any calls yet
        self.assertEqual(0, self.client.calls_today)
        with requests_mock.Mocker(real_http=True) as mock:
            # the endpoitn we're going to call to make sure call count was updated
            mock.register_uri("GET", self.client.get_url("what"), json={"success": True})
            # mock out the call count endpoint
            mock.register_uri("GET", self.client.get_url("rest/v1/stats/usage.json"), json={"result": [{"total": 200}]})
            # make the request
            self.client.request("GET", "what")

        # call count should be updated
        self.assertEqual(201, self.client.calls_today)

    def test_over_quota_raises_exception(self):
        # disable refresh_token being called
        self.client.token_expires = pendulum.utcnow().add(days=1)
        self.client.calls_today = self.client.max_daily_calls + 1
        with self.assertRaises(ApiException):
            self.client.request("GET", "it")

    def test_test_corona(self):
        # disable refresh_token being called
        self.client.token_expires = pendulum.utcnow().add(days=1)
        # disable calls_today
        self.client.calls_today = 1
        with requests_mock.Mocker(real_http=True) as mock:
            create = self.client.get_bulk_endpoint("leads", "create")
            cancel = self.client.get_bulk_endpoint("leads", "cancel", "123")
            mock.register_uri("POST", self.client.get_url(create), json={"success": True, "result": [{"exportId": "123"}]})
            mock.register_uri("POST", self.client.get_url(cancel), json={"success": True})
            self.assertTrue(self.client.use_corona)

    def test_test_corona_unsupported(self):
        # disable refresh_token being called
        self.client.token_expires = pendulum.utcnow().add(days=1)
        # disable calls_today
        self.client.calls_today = 1
        with requests_mock.Mocker(real_http=True) as mock:
            create = self.client.get_bulk_endpoint("leads", "create")
            cancel = self.client.get_bulk_endpoint("leads", "cancel", "123")
            mock.register_uri("POST", self.client.get_url(create), json={"errors": [{"code": "1035"}]})
            self.assertFalse(self.client.use_corona)

    def test_raise_for_status_forbidden(self):
        """Ensures a raw HTTP 403 is normalized to MarketoForbiddenError by _request,
        matching the application-error-envelope (code 603) path in request()."""
        self.client.token_expires = pendulum.utcnow().add(days=1)
        self.client.access_token = "token"
        response = unittest.mock.MagicMock()
        response.status_code = 403
        response.raise_for_status.side_effect = requests.exceptions.HTTPError("403")

        self.client._session.send = unittest.mock.MagicMock(return_value=response)

        with self.assertRaises(MarketoForbiddenError):
            self.client._request("GET", "rest/v1/forbidden.json")

    def test_raise_for_status_non_forbidden_still_raises_http_error(self):
        """Ensures non-403 HTTP errors still surface as requests.HTTPError (unaffected
        by the 403-to-MarketoForbiddenError normalization)."""
        self.client.token_expires = pendulum.utcnow().add(days=1)
        self.client.access_token = "token"
        response = unittest.mock.MagicMock()
        response.status_code = 404
        response.raise_for_status.side_effect = requests.exceptions.HTTPError("404")

        self.client._session.send = unittest.mock.MagicMock(return_value=response)

        with self.assertRaises(requests.exceptions.HTTPError):
            self.client._request("GET", "rest/v1/missing.json")

    def test_raise_for_status_forbidden_bounds_and_sanitizes_response_body(self):
        """Regression test: the MarketoForbiddenError message embeds a bounded,
        sanitized summary of the response body (not the raw content), since
        this exception's message is logged verbatim by discover.py/sync.py."""
        self.client.token_expires = pendulum.utcnow().add(days=1)
        self.client.access_token = "token"
        response = unittest.mock.MagicMock()
        response.status_code = 403
        response.raise_for_status.side_effect = requests.exceptions.HTTPError("403")
        huge_body = ("x" * 5000).encode("utf-8")
        response.content = huge_body

        self.client._session.send = unittest.mock.MagicMock(return_value=response)

        with self.assertRaises(MarketoForbiddenError) as ctx:
            self.client._request("GET", "rest/v1/forbidden.json")

        message = str(ctx.exception)
        self.assertLess(len(message), len(huge_body))
        self.assertIn("truncated", message)
        self.assertIn("5000 bytes total", message)

    def test_raise_for_status_forbidden_strips_control_characters_from_body(self):
        """Ensures newlines/control characters in the response body can't be
        used to forge extra log lines via the MarketoForbiddenError message."""
        self.client.token_expires = pendulum.utcnow().add(days=1)
        self.client.access_token = "token"
        response = unittest.mock.MagicMock()
        response.status_code = 403
        response.raise_for_status.side_effect = requests.exceptions.HTTPError("403")
        response.content = b"real error\nCRITICAL fake injected log line"

        self.client._session.send = unittest.mock.MagicMock(return_value=response)

        with self.assertRaises(MarketoForbiddenError) as ctx:
            self.client._request("GET", "rest/v1/forbidden.json")

        message = str(ctx.exception)
        self.assertNotIn("\n", message)
        self.assertIn("real error CRITICAL fake injected log line", message)


class TestSummarizeResponseBody(unittest.TestCase):
    """Covers the bounded/sanitized response-body summary helper used when
    building exception messages from upstream HTTP responses."""

    def test_summarize_response_body_passes_through_short_text(self):
        """Ensures short, clean bodies are returned unmodified."""
        response = unittest.mock.MagicMock()
        response.content = b"Forbidden: no access"
        self.assertEqual("Forbidden: no access", summarize_response_body(response))

    def test_summarize_response_body_handles_empty_content(self):
        """Ensures an empty body produces a clear placeholder instead of an
        empty/confusing message."""
        response = unittest.mock.MagicMock()
        response.content = b""
        self.assertEqual("<empty response body>", summarize_response_body(response))

    def test_summarize_response_body_truncates_long_content(self):
        """Ensures long bodies are bounded to MAX_RESPONSE_BODY_LOG_LENGTH and
        annotated with the original size."""
        response = unittest.mock.MagicMock()
        response.content = ("a" * 1000).encode("utf-8")

        summary = summarize_response_body(response)

        self.assertLessEqual(len(summary), MAX_RESPONSE_BODY_LOG_LENGTH + len("... (truncated, 1000 bytes total)"))
        self.assertIn("truncated, 1000 bytes total", summary)

    def test_summarize_response_body_strips_control_characters(self):
        """Ensures newlines and other control characters are collapsed so the
        summary can't inject extra log lines."""
        response = unittest.mock.MagicMock()
        response.content = b"line one\nline two\r\ttabbed"

        summary = summarize_response_body(response)

        self.assertNotIn("\n", summary)
        self.assertNotIn("\r", summary)
        self.assertNotIn("\t", summary)

    def test_summarize_response_body_handles_non_utf8_bytes(self):
        """Ensures undecodable bytes don't raise a secondary exception while
        building the summary."""
        response = unittest.mock.MagicMock()
        response.content = b"\xff\xfe invalid utf-8 \xff"

        # Should not raise.
        summary = summarize_response_body(response)
        self.assertIsInstance(summary, str)


class TestExports(unittest.TestCase):
    """Covers bulk export lifecycle polling and file streaming behavior."""

    def setUp(self):
        self.client = Client("123-ABC-456", "id", "secret")
        self.client.token_expires = pendulum.utcnow().add(days=1)
        self.client.calls_today = 1

    def test_export_enqueued(self):
        """Verifies wait_for_export enqueues and eventually completes a job."""
        export_id = "123"
        self.client.poll_interval = 0
        self.client.poll_export = unittest.mock.MagicMock(side_effect=["Created", "Completed"])
        self.client.enqueue_export = unittest.mock.MagicMock()

        self.assertTrue(self.client.wait_for_export("test", export_id))
        self.client.enqueue_export.assert_called_once_with("test", export_id)

    def test_api_exception(self):
        """Ensures API exceptions during polling are re-raised by wait_for_export."""
        export_id = "123"
        self.client.poll_interval = 0
        self.client.poll_export = unittest.mock.MagicMock(side_effect=ApiException("Oh no!"))

        with self.assertRaises(ApiException):
            self.client.wait_for_export("test", export_id)

    def test_export_timed_out(self):
        """Ensures wait_for_export raises ExportFailed when timeout is exceeded."""
        export_id = "123"
        self.client.poll_interval = 0
        self.client.job_timeout = 0
        self.client.poll_export = unittest.mock.MagicMock(side_effect=itertools.repeat("Queued"))

        with self.assertRaises(ExportFailed):
            self.client.wait_for_export("test", export_id)

    def test_export_failed(self):
        """Ensures failed export statuses raise ExportFailed."""
        export_id = "123"
        self.client.poll_interval = 0
        self.client.poll_export = unittest.mock.MagicMock(side_effect=["Failed"])

        with self.assertRaises(ExportFailed):
            self.client.wait_for_export("test", export_id)

    def _file_url(self, export_id="exp-1"):
        return self.client.get_url(self.client.get_bulk_endpoint("leads", "file", export_id))

    def test_stream_export_returns_streaming_response_for_csv(self):
        """Verifies CSV export responses are returned as streaming content."""
        self.client.access_token = "token"
        with requests_mock.Mocker(real_http=True) as mock:
            mock.register_uri("GET", self._file_url(), content=b"id,name\n1,Alice\n",
                              headers={"Content-Type": "text/csv;charset=UTF-8"})
            resp = self.client.stream_export("leads", "exp-1")
        self.assertEqual(b"id,name\n1,Alice\n", resp.content)

    def test_stream_export_raises_on_json_error_envelope(self):
        """A 200 with a JSON error body must not be passed back to be parsed as CSV."""
        self.client.access_token = "token"
        with requests_mock.Mocker(real_http=True) as mock:
            mock.register_uri(
                "GET", self._file_url(),
                json={"requestId": "x", "success": False,
                      "errors": [{"code": "1035", "message": "no corona"}]},
                headers={"Content-Type": "application/json"})
            with self.assertRaises(ApiException):
                self.client.stream_export("leads", "exp-1")

    def test_stream_export_raises_marketo_forbidden_on_access_denied_envelope(self):
        """Regression test: a 200 export-file response carrying the access-denied
        envelope (code 603) must raise MarketoForbiddenError, not a plain
        ApiException, so sync.py's per-activity-type permission handling
        (which only catches MarketoForbiddenError around sync_activities())
        records the stream as denied and continues syncing other selected
        streams instead of aborting the whole sync."""
        self.client.access_token = "token"
        with requests_mock.Mocker(real_http=True) as mock:
            mock.register_uri(
                "GET", self._file_url(),
                json={"requestId": "x", "success": False,
                      "errors": [{"code": "603", "message": "Access denied"}]},
                headers={"Content-Type": "application/json"})
            with self.assertRaises(MarketoForbiddenError):
                self.client.stream_export("leads", "exp-1")

    @unittest.mock.patch("time.sleep")
    def test_stream_export_retries_short_term_quota_then_succeeds(self, _sleep):
        """A 606 rate-limit envelope should back off and retry, then return the file."""
        self.client.access_token = "token"
        with requests_mock.Mocker(real_http=True) as mock:
            mock.register_uri("GET", self._file_url(), [
                {"json": {"requestId": "x", "success": False,
                          "errors": [{"code": "606", "message": "rate limited"}]},
                 "headers": {"Content-Type": "application/json"}},
                {"content": b"id,name\n1,Alice\n",
                 "headers": {"Content-Type": "text/csv"}},
            ])
            resp = self.client.stream_export("leads", "exp-1")
            self.assertEqual(b"id,name\n1,Alice\n", resp.content)
            self.assertEqual(2, mock.call_count)

    @unittest.mock.patch("time.sleep")
    def test_stream_export_retries_on_615_concurrent_access_limit(self, _sleep):
        # A 615 envelope (concurrent access limit reached) should raise
        # ConcurrentAccessLimitError and be retried by the backoff decorator.
        self.client.access_token = "token"
        with requests_mock.Mocker(real_http=True) as mock:
            mock.register_uri("GET", self._file_url(), [
                {"json": {"requestId": "x", "success": False,
                          "errors": [{"code": "615", "message": "Concurrent access limit '10' reached"}]},
                 "headers": {"Content-Type": "application/json"}},
                {"content": b"id,name\n1,Alice\n",
                 "headers": {"Content-Type": "text/csv"}},
            ])
            resp = self.client.stream_export("leads", "exp-1")
            self.assertEqual(b"id,name\n1,Alice\n", resp.content)
            self.assertEqual(2, mock.call_count)

    @unittest.mock.patch("time.sleep")
    def test_raise_for_rate_limit_raises_concurrent_access_limit_error(self, _sleep):
        # Ensure raise_for_rate_limit raises ConcurrentAccessLimitError for code 615.
        data = {"errors": [{"code": "615", "message": "Concurrent access limit '10' reached"}]}
        with self.assertRaises(ConcurrentAccessLimitError):
            raise_for_rate_limit(data)
