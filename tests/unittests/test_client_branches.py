import unittest
from unittest.mock import Mock, patch

from tap_marketo.client import (
    API_QUOTA_EXCEEDED_MESSAGE,
    ApiException,
    ApiQuotaExceeded,
    Client,
    MarketoForbiddenError,
    ShortTermQuotaExceeded,
    SHORT_TERM_QUOTA_EXCEEDED_MESSAGE,
    raise_for_rate_limit,
)


class TestClientBranches(unittest.TestCase):
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

    def test_raise_for_status_403(self):
        client = Client("123-ABC-789", "id", "secret")
        response = Mock(status_code=403)
        with self.assertRaises(MarketoForbiddenError):
            client._raise_for_status(response)

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
