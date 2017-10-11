import itertools
import logging
import unittest
import unittest.mock

import freezegun
import pendulum
import requests_mock

from tap_marketo.client import *


logging.disable(logging.CRITICAL)


class TestClient(unittest.TestCase):
    def setUp(self):
        self.client = Client("123-ABC-789", "id", "secret")

    def test_extract_domain(self):
        self.assertEqual("123-ABC-789", extract_domain("https://123-ABC-789.mktorest.com/rest"))
        with self.assertRaises(ValueError):
            extract_domain("notadomain")

    @freezegun.freeze_time("2017-01-01")
    def test_refresh_token(self):
        with requests_mock.Mocker(real_http=True) as mock:
            mock.register_uri("GET", self.client.get_url("identity/oauth/token"), json={"access_token": "token", "expires_in": 1800})
            self.client.refresh_token()

        expires = pendulum.datetime(2017, 1, 1).add(seconds=1800 - 15)
        self.assertEqual("token", self.client.access_token)
        self.assertEqual(expires, self.client.token_expires)

    def test_refresh_token_error_not_2xx(self):
        with requests_mock.Mocker(real_http=True) as mock:
            mock.register_uri("GET", self.client.get_url("identity/oauth/token"), status_code=404)
            with self.assertRaises(ApiException):
                self.client.refresh_token()

    def test_refresh_token_error_raises_exception(self):
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


class TestExports(unittest.TestCase):
    def setUp(self):
        self.client = Client("123-ABC-456", "id", "secret")
        self.client.token_expires = pendulum.utcnow().add(days=1)
        self.client.calls_today = 1

    def test_export_enqueued(self):
        export_id = "123"
        self.client.poll_interval = 0
        self.client.poll_export = unittest.mock.MagicMock(side_effect=["Created", "Completed"])
        self.client.enqueue_export = unittest.mock.MagicMock()

        self.assertTrue(self.client.wait_for_export("test", export_id))
        self.client.enqueue_export.assert_called_once_with("test", export_id)

    def test_api_exception(self):
        export_id = "123"
        self.client.poll_interval = 0
        self.client.poll_export = unittest.mock.MagicMock(side_effect=ApiException("Oh no!"))

        with self.assertRaises(ApiException):
            self.client.wait_for_export("test", export_id)

    def test_export_timed_out(self):
        export_id = "123"
        self.client.poll_interval = 0
        self.client.job_timeout = 0
        self.client.poll_export = unittest.mock.MagicMock(side_effect=itertools.repeat("Queued"))

        with self.assertRaises(ExportFailed):
            self.client.wait_for_export("test", export_id)

    def test_export_failed(self):
        export_id = "123"
        self.client.poll_interval = 0
        self.client.poll_export = unittest.mock.MagicMock(side_effect=["Failed"])

        with self.assertRaises(ExportFailed):
            self.client.wait_for_export("test", export_id)
