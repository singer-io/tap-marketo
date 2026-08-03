"""Branch-focused unit tests for less common tap_marketo client paths."""

import unittest
from unittest.mock import Mock, patch
import requests

from tap_marketo.client import (
    API_QUOTA_EXCEEDED_MESSAGE,
    ApiException,
    ApiQuotaExceeded,
    Client,
    ShortTermQuotaExceeded,
    SHORT_TERM_QUOTA_EXCEEDED_MESSAGE,
    raise_for_rate_limit,
)


class TestClientBranches(unittest.TestCase):
    """Exercises error branches and wrapper paths for client methods."""

    @patch("tap_marketo.client.pendulum.now")
    @patch("tap_marketo.client.pendulum.utcnow", side_effect=AttributeError("utcnow"))
    def test_utcnow_falls_back_to_pendulum_now(self, _mock_utcnow, mock_now):
        from tap_marketo.client import utcnow

        sentinel = object()
        mock_now.return_value = sentinel

        self.assertIs(sentinel, utcnow())
        mock_now.assert_called_once_with("UTC")

    def test_raise_for_rate_limit_api_quota(self):
        data = {"errors": [{"code": "1029", "message": "quota"}]}
        with self.assertRaises(ApiQuotaExceeded) as ctx:
            raise_for_rate_limit(data)
        self.assertIn(API_QUOTA_EXCEEDED_MESSAGE.split("{}")[0], str(ctx.exception))

    @patch("tap_marketo.client.singer.log_warning")
    def test_raise_for_rate_limit_short_term(self, log_warning):
        data = {"errors": [{"code": "606", "message": "short"}]}
        with self.assertRaises(ShortTermQuotaExceeded) as ctx:
            raise_for_rate_limit(data)
        self.assertIn(SHORT_TERM_QUOTA_EXCEEDED_MESSAGE.split("{}")[0], str(ctx.exception))
        log_warning.assert_called_once()

    def test_request_returns_empty_dict_for_empty_content(self):
        client = Client("123-ABC-789", "id", "secret")
        client.calls_today = 1
        client.token_expires = object()

        response = Mock()
        response.content = b""

        with patch.object(client, "_request", return_value=response):
            result = client.request("GET", "rest/v1/foo.json")

        self.assertEqual({}, result)

    def test_request_stream_mode_raises_on_non_200_206(self):
        client = Client("123-ABC-789", "id", "secret")
        client.calls_today = 1
        client.token_expires = object()

        response = Mock()
        response.status_code = 500
        response.content = b"bad"

        with patch.object(client, "_request", return_value=response):
            with self.assertRaises(ApiException):
                client.request("GET", "rest/v1/foo.json", stream=True)

    def test_refresh_token_unauthorized_error_message(self):
        client = Client("123-ABC-789", "id", "secret")
        mock_resp = Mock(status_code=200)
        mock_resp.json.return_value = {"error": "unauthorized", "error_description": "bad creds"}

        with patch("tap_marketo.client.requests.get", return_value=mock_resp):
            with self.assertRaises(ApiException) as err:
                Client.refresh_token.__wrapped__(client)

        self.assertIn("Authorization failed", str(err.exception))

    def test_refresh_token_connection_error_raises_api_exception(self):
        client = Client("123-ABC-789", "id", "secret")

        with patch("tap_marketo.client.requests.get", side_effect=requests.exceptions.ConnectionError("down")):
            with self.assertRaises(ApiException):
                Client.refresh_token.__wrapped__(client)

    def test_update_calls_today_missing_result_raises(self):
        client = Client("123-ABC-789", "id", "secret")
        with patch.object(client, "_request") as mock_request:
            mock_request.return_value.json.return_value = {"success": True}
            with self.assertRaises(ApiException):
                client.update_calls_today()

    def test_request_raises_on_unsuccessful_payload(self):
        client = Client("123-ABC-789", "id", "secret")
        client.calls_today = 1
        client.token_expires = object()

        response = Mock()
        response.content = b'{"success": false}'
        response.json.return_value = {
            "success": False,
            "errors": [{"code": "400", "message": "bad"}],
        }

        with patch.object(client, "_request", return_value=response):
            with self.assertRaises(ApiException):
                client.request("GET", "rest/v1/foo.json")

    def test_create_enqueue_cancel_and_status_wrappers(self):
        client = Client("123-ABC-789", "id", "secret")

        with patch.object(client, "request") as mock_request:
            mock_request.return_value = {"result": [{"exportId": "exp-1"}]}
            export_id = client.create_export("leads", ["id"], {"updatedAt": {"startAt": "a", "endAt": "b"}})
            self.assertEqual("exp-1", export_id)

            client.enqueue_export("leads", "exp-1")
            client.cancel_export("leads", "exp-1")

            mock_request.return_value = {"result": [{"status": "Completed"}]}
            self.assertEqual({"result": [{"status": "Completed"}]}, client.get_export_status("leads", "exp-1"))
            self.assertEqual("Completed", client.poll_export("leads", "exp-1"))

    def test_get_existing_exports_and_export_available_paths(self):
        client = Client("123-ABC-789", "id", "secret")

        with patch.object(client, "request", return_value={"result": [{"exportId": "exp-1", "status": "Completed"}]}) as mock_request:
            exports = client.get_existing_exports("leads")
        self.assertIn("exp-1", exports)
        mock_request.assert_called_once()

        with patch.object(client, "request", return_value={"success": True}):
            self.assertEqual({}, client.get_existing_exports("leads"))

        with patch.object(client, "get_existing_exports", return_value={"exp-1": {"status": "Completed"}}), \
                patch.object(client, "export_file_exists", return_value=True):
            self.assertTrue(client.export_available("leads", "exp-1"))

        with patch.object(client, "get_existing_exports", return_value={}):
            self.assertFalse(client.export_available("leads", "exp-missing"))

    def test_export_file_exists_paths(self):
        client = Client("123-ABC-789", "id", "secret")

        self.assertTrue(client.export_file_exists("leads", "exp-1", {"exp-1": {"status": "Queued"}}))

        ok_resp = Mock()
        with patch.object(client, "request", return_value=ok_resp):
            self.assertTrue(client.export_file_exists("leads", "exp-1", {"exp-1": {"status": "Completed"}}))
        ok_resp.close.assert_called_once()

        not_found = requests.exceptions.HTTPError("404")
        not_found.response = Mock(status_code=404)
        with patch.object(client, "request", side_effect=not_found):
            self.assertFalse(client.export_file_exists("leads", "exp-1", {"exp-1": {"status": "Completed"}}))

        server_error = requests.exceptions.HTTPError("500")
        server_error.response = Mock(status_code=500)
        with patch.object(client, "request", side_effect=server_error):
            with self.assertRaises(requests.exceptions.HTTPError):
                client.export_file_exists("leads", "exp-1", {"exp-1": {"status": "Completed"}})

    def test_stream_export_resume_headers(self):
        client = Client("123-ABC-789", "id", "secret")
        response = Mock()
        response.headers = {"Content-Type": "text/csv"}

        with patch.object(client, "request", return_value=response) as mock_request:
            client.stream_export("leads", "exp-1", start_byte=25)

        self.assertEqual({"Range": "bytes=25-"}, mock_request.call_args.kwargs["headers"])
