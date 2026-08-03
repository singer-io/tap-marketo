"""Unit tests for request timeout configuration and retry/backoff handling."""

import unittest
import requests
import pendulum
from unittest import mock

from tap_marketo.client import Client

# Mock response object
def get_mock_http_response(*args, **kwargs):
    contents = '{"access_token": "test", "expires_in":100}'
    response = requests.Response()
    response.status_code = 200
    response._content = contents.encode()
    return response

# Mock request object
class MockRequest:
    """Simple request payload holder used by timeout behavior tests."""

    def __init__(self):
        self.url = "test"
mock_request_object = MockRequest()

@mock.patch('requests.Session.send')
@mock.patch("requests.Request.prepare")
@mock.patch("requests.get", side_effect = get_mock_http_response)
class TestRequestTimeoutValue(unittest.TestCase):
    """Verifies request_timeout coercion and defaulting behavior."""


    def test_no_request_timeout_in_config(self, mocked_get, mocked_prepare, mocked_send):
        # Ensures default timeout is used when request_timeout is not provided.
        """
            Verify that if request_timeout is not provided in config then default value is used
        """
        config = {   # No request_timeout in config
            "endpoint": "123-ABC-789",
            "client_id": "test",
            "client_secret": "test"
        }
        mocked_prepare.return_value = mock_request_object

        # Initialize Client object which set value for self.request_timeout
        client = Client(**config)
        client.token_expires = pendulum.utcnow().add(days=1)
        # Verify request_timeout is set with expected value
        self.assertEqual(client.request_timeout, 300.0)

        # Call _request method which call session.send with timeout
        client._request("test", "test")
        # Verify session.send is called with expected timeout
        mocked_send.assert_called_with(mock_request_object, stream=False, timeout=300.0)

        # Call refresh_token method which call requests.get with timeout
        client.refresh_token()
        # Verify requests.get is called with expected timeout
        mocked_get.assert_called_with('https://123-ABC-789.mktorest.com/identity/oauth/token',
                                      params={'grant_type': 'client_credentials', 'client_id': 'test', 'client_secret': 'test'},
                                      timeout=300.0)

    def test_integer_request_timeout_in_config(self, mocked_get, mocked_prepare, mocked_send):
        # Ensures integer timeout values are accepted and used in all HTTP paths.
        """
            Verify that if request_timeout is provided in config(integer value) then it should be use
        """
        config = {
            "endpoint": "123-ABC-789",
            "client_id": "test",
            "client_secret": "test",
            "request_timeout": 100 # integer timeout in config
        }
        mocked_prepare.return_value = mock_request_object

        # Initialize Client object which set value for self.request_timeout
        client = Client(**config)
        client.token_expires = pendulum.utcnow().add(days=1)
        # Verify request_timeout is set with expected value
        self.assertEqual(client.request_timeout, 100.0)

        # Call _request method which call session.send with timeout
        client._request("test", "test")
        # Verify session.send is called with expected timeout
        mocked_send.assert_called_with(mock_request_object, stream=False, timeout=100.0)

        # Call refresh_token method which call requests.get with timeout
        client.refresh_token()
        # Verify requests.get is called with expected timeout
        mocked_get.assert_called_with('https://123-ABC-789.mktorest.com/identity/oauth/token',
                                      params={'grant_type': 'client_credentials', 'client_id': 'test', 'client_secret': 'test'},
                                      timeout=100.0)

    def test_float_request_timeout_in_config(self, mocked_get, mocked_prepare, mocked_send):
        # Ensures float timeout values are accepted and used in all HTTP paths.
        """
            Verify that if request_timeout is provided in config(float value) then it should be use
        """
        config = {
            "endpoint": "123-ABC-789",
            "client_id": "test",
            "client_secret": "test",
            "request_timeout": 100.5 # float timeout in config
        }
        mocked_prepare.return_value = mock_request_object

        # Initialize Client object which set value for self.request_timeout
        client = Client(**config)
        client.token_expires = pendulum.utcnow().add(days=1)
        # Verify request_timeout is set with expected value
        self.assertEqual(client.request_timeout, 100.5)

        # Call _request method which call session.send with timeout
        client._request("test", "test")
        # Verify session.send is called with expected timeout
        mocked_send.assert_called_with(mock_request_object, stream=False, timeout=100.5)

        # Call refresh_token method which call requests.get with timeout
        client.refresh_token()
        # Verify requests.get is called with expected timeout
        mocked_get.assert_called_with('https://123-ABC-789.mktorest.com/identity/oauth/token',
                                      params={'grant_type': 'client_credentials', 'client_id': 'test', 'client_secret': 'test'},
                                      timeout=100.5)

    def test_string_request_timeout_in_config(self, mocked_get, mocked_prepare, mocked_send):
        # Ensures numeric string timeout values are coerced and used correctly.
        """
            Verify that if request_timeout is provided in config(string value) then it should be use
        """
        config = {
            "endpoint": "123-ABC-789",
            "client_id": "test",
            "client_secret": "test",
            "request_timeout": '100' # string format timeout in config
        }
        mocked_prepare.return_value = mock_request_object

        # Initialize Client object which set value for self.request_timeout
        client = Client(**config)
        client.token_expires = pendulum.utcnow().add(days=1)
        # Verify request_timeout is set with expected value
        self.assertEqual(client.request_timeout, 100.0)

        # Call _request method which call session.send with timeout
        client._request("test", "test")
        # Verify session.send is called with expected timeout
        mocked_send.assert_called_with(mock_request_object, stream=False, timeout=100.0)

        # Call refresh_token method which call requests.get with timeout
        client.refresh_token()
        # Verify requests.get is called with expected timeout
        mocked_get.assert_called_with('https://123-ABC-789.mktorest.com/identity/oauth/token',
                                      params={'grant_type': 'client_credentials', 'client_id': 'test', 'client_secret': 'test'},
                                      timeout=100.0)

    def test_empty_string_request_timeout_in_config(self, mocked_get, mocked_prepare, mocked_send):
        # Ensures empty string timeout values fall back to default timeout.
        """
            Verify that if request_timeout is provided in config with empty string then default value is used
        """
        config = {
            "endpoint": "123-ABC-789",
            "client_id": "test",
            "client_secret": "test",
            "request_timeout": '' # empty string in config
        }
        mocked_prepare.return_value = mock_request_object

        # Initialize Client object which set value for self.request_timeout
        client = Client(**config)
        client.token_expires = pendulum.utcnow().add(days=1)
        # Verify request_timeout is set with expected value
        self.assertEqual(client.request_timeout, 300.0)

        # Call _request method which call session.send with timeout
        client._request("test", "test")
        # Verify session.send is called with expected timeout
        mocked_send.assert_called_with(mock_request_object, stream=False, timeout=300.0)

        # Call refresh_token method which call requests.get with timeout
        client.refresh_token()
        # Verify requests.get is called with expected timeout
        mocked_get.assert_called_with('https://123-ABC-789.mktorest.com/identity/oauth/token',
                                      params={'grant_type': 'client_credentials', 'client_id': 'test', 'client_secret': 'test'},
                                      timeout=300.0)

    def test_zero_request_timeout_in_config(self, mocked_get, mocked_prepare, mocked_send):
        # Ensures numeric zero timeout is treated as invalid and reset to default.
        """
            Verify that if request_timeout is provided in config with zero value then default value is used
        """
        config = {
            "endpoint": "123-ABC-789",
            "client_id": "test",
            "client_secret": "test",
            "request_timeout": 0.0 # zero value in config
        }
        mocked_prepare.return_value = mock_request_object

        # Initialize Client object which set value for self.request_timeout
        client = Client(**config)
        client.token_expires = pendulum.utcnow().add(days=1)
        # Verify request_timeout is set with expected value
        self.assertEqual(client.request_timeout, 300.0)

        # Call _request method which call session.send with timeout
        client._request("test", "test")
        # Verify session.send is called with expected timeout
        mocked_send.assert_called_with(mock_request_object, stream=False, timeout=300.0)

        # Call refresh_token method which call requests.get with timeout
        client.refresh_token()
        # Verify requests.get is called with expected timeout
        mocked_get.assert_called_with('https://123-ABC-789.mktorest.com/identity/oauth/token',
                                      params={'grant_type': 'client_credentials', 'client_id': 'test', 'client_secret': 'test'},
                                      timeout=300.0)

    def test_zero_string_request_timeout_in_config(self, mocked_get, mocked_prepare, mocked_send):
        # Ensures zero-like timeout strings are treated as invalid and reset to default.
        """
            Verify that if request_timeout is provided in config with zero in string format then default value is used
        """
        config = {
            "endpoint": "123-ABC-789",
            "client_id": "test",
            "client_secret": "test",
            "request_timeout": '0.0' # zero value in config
        }
        mocked_prepare.return_value = mock_request_object

        # Initialize Client object which set value for self.request_timeout
        client = Client(**config)
        client.token_expires = pendulum.utcnow().add(days=1)
        # Verify request_timeout is set with expected value
        self.assertEqual(client.request_timeout, 300.0)

        # Call _request method which call session.send with timeout
        client._request("test", "test")
        # Verify session.send is called with expected timeout
        mocked_send.assert_called_with(mock_request_object, stream=False, timeout=300.0)

        # Call refresh_token method which call requests.get with timeout
        client.refresh_token()
        # Verify requests.get is called with expected timeout
        mocked_get.assert_called_with('https://123-ABC-789.mktorest.com/identity/oauth/token',
                                      params={'grant_type': 'client_credentials', 'client_id': 'test', 'client_secret': 'test'},
                                      timeout=300.0)


@mock.patch("time.sleep")
class TestRequestTimeoutBackoff(unittest.TestCase):
    # Validates timeout-triggered retry behavior in auth and request paths.
    """Validates timeout-triggered retry behavior for request functions."""


    @mock.patch("requests.get", side_effect = requests.exceptions.Timeout)
    def test_request_timeout_backoff_in_refresh_token(self, mocked_request, mocked_sleep):
        # Verifies refresh_token retries the expected number of times on timeout.
        """
            Verify refresh_token function is backoff for 5 times on Timeout exceeption
        """
        config = {
            "endpoint": "123-ABC-789",
            "client_id": "test",
            "client_secret": "test"
        }
        # Initialize Client object
        client = Client(**config)
        client.token_expires = pendulum.utcnow().add(days=1)

        try:
            client.refresh_token()
        except requests.exceptions.Timeout:
            pass
        # Verify that requests.get is called 5 times
        self.assertEqual(mocked_request.call_count, 5)

    @mock.patch('requests.Session.send', side_effect = requests.exceptions.Timeout)
    def test_request_timeout_backoff_in__request_function(self, mocked_send, mocked_sleep):
        # Verifies _request retries the expected number of times on timeout.
        """
            Verify _request function is backoff for 5 times on Timeout exceeption
        """
        config = {
            "endpoint": "123-ABC-789",
            "client_id": "test",
            "client_secret": "test"
        }
        # Initialize Client object
        client = Client(**config)
        client.token_expires = pendulum.utcnow().add(days=1)

        try:
            client._request("test", "test")
        except requests.exceptions.Timeout:
            pass
        # Verify that session.send is called 5 times
        self.assertEqual(mocked_send.call_count, 5)
