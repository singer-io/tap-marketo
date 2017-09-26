import time

import pendulum
import requests
import singer


JOB_TIMEOUT = 60 * 30
POLL_INTERVAL = 60 * 3

LOGGER = singer.get_logger()


class ApiException(Exception):
    pass


class ExportFailed(Exception):
    pass


class Client:
    def __init__(self, domain, client_id, client_secret,
                 max_daily_calls=8000, user_agent="Singer.io/tap-marketo"):

        self.domain = domain
        self.client_id = client_id
        self.client_secret = client_secret
        self.max_daily_calls = int(max_daily_calls)
        self.user_agent = user_agent

        self.token_expires = None
        self.access_token = None
        self.calls_today = 0

        self._session = requests.Session()
        self.refresh_token()

    @classmethod
    def from_config(cls, config):
        return cls(
            domain=config["domain"],
            client_id=config["client_id"],
            client_secret=config["client_secret"],
            max_daily_calls=config.get("max_daily_calls"),
            user_agent=config.get("user_agent"),
        )

    @property
    def headers(self):
        if not self.token_expires or self.token_expires <= pendulum.utcnow():
            raise Exception("Must refresh token first")

        return {
            "Authorization": "Bearer {}".format(self.access_token),
            "User-Agent": self.user_agent,
        }

    def get_url(self, url):
        return "https://{}.mktorest.com/{}".format(self.domain, url)

    def get_bulk_endpoint(self, stream_name, action, export_id=None):
        endpoint = "bulk/v1/{}/export/".format(stream_name)
        if export_id is not None:
            endpoint += "{}/".format(export_id)

        endpoint += "{}.json".format(action)
        return endpoint

    def refresh_token(self):
        params = {
            "grant_type": "client_credentials",
            "client_id": self.client_id,
            "client_secret": self.client_secret,
        }
        LOGGER.info("Refreshing token")

        try:
            url = self.get_url("identity/oauth/token")
            resp = requests.get(url, params=params)
            resp_time = pendulum.utcnow()
        except requests.exceptions.ConnectionError:
            raise ApiException("Connection error while refreshing token at %s.", url)

        if resp.status_code != 200:
            resp.raise_for_status()

        data = resp.json()
        if "error" in data:
            if data["error"] == "unauthorized":
                msg = "Authorization failed: "
            else:
                msg = "API returned an error: "

            msg += data.get("error_description", "No message from api")
            raise ApiException(msg)

        self.access_token = data["access_token"]
        self.token_expires = resp_time.add(seconds=data["expires_in"] - 15)
        LOGGER.info("Token valid until %s", self.token_expires)

    def _request(self, method, url, stream=False, **kwargs):
        headers = kwargs.pop("headers", {})
        headers.update(self.headers)
        req = requests.Request(method, url, headers=headers, **kwargs).prepare()
        LOGGER.info("%s: %s", method, req.url)
        with singer.metrics.http_request_timer(url):
            resp = self._session.send(req, stream=stream)

        if resp.status_code >= 400:
            raise ApiException(resp)

        return resp

    def update_calls_today(self):
        data = self._request("GET", self.get_url("rest/v1/stats/usage.json")).json()
        if "result" not in data:
            raise ApiException(data)

        self.calls_today = int(data["result"][0]["total"])
        LOGGER.info("Used %s of %s requests", self.calls_today, self.max_daily_calls)

    @singer.utils.ratelimit(100, 20)
    @singer.utils.backoff((requests.exceptions.RequestException), singer.utils.exception_is_4xx)
    def request(self, method, url, **kwargs):
        if not self.token_expires or self.token_expires <= pendulum.utcnow():
            self.refresh_token()

        if self.calls_today % 250 == 0:
            self.update_calls_today()

        self.calls_today += 1
        if self.calls_today > self.max_daily_calls:
            raise ApiException("Exceeded daily quota of %s calls", self.max_daily_calls)

        resp = self._request(method, self.get_url(url), **kwargs)
        if "stream" not in kwargs:
            data = resp.json()
            if not data["success"]:
                err = ", ".join("{code}: {message}".format(**e) for e in data["errors"])
                raise ApiException("API returned error(s): {}".format(err))

            return data
        else:
            if resp.status_code != 200:
                raise ApiException("API returned error: {0.status_code}: {0.content}".format(resp))

            return resp.iter_lines()

    def create_export(self, stream_type, fields=None, query=None):
        payload = {"format": "CSV"}
        if fields:
            payload["fields"] = fields

        if query:
            payload["filter"] = query

        endpoint = self.get_bulk_endpoint(stream_type, "create")
        return self.request("POST", endpoint, json=payload)["result"][0]["exportId"]

    def enqueue_export(self, stream_type, export_id):
        endpoint = self.get_bulk_endpoint(stream_type, "enqueue", export_id)
        self.request("POST", endpoint)

    def poll_export(self, stream_type, export_id):
        endpoint = self.get_bulk_endpoint(stream_type, "status", export_id)
        return self.request("GET", endpoint)["result"][0]["status"]

    def stream_export(self, stream_type, export_id):
        endpoint = self.get_bulk_endpoint(stream_type, "file", export_id)
        return self.request("GET", endpoint, stream=True)

    def wait_for_export(self, stream_type, export_id):
        timeout_time = pendulum.utcnow().add(seconds=JOB_TIMEOUT)
        while pendulum.utcnow() < timeout_time:
            endpoint = self.get_bulk_endpoint(stream_type, "status", export_id)
            status = self.request("GET", endpoint)["result"][0]["status"]

            if status == "Created":
                self.request("POST", self.get_bulk_endpoint("activities", "enqueue", export_id))

            elif status in ["Cancelled", "Failed"]:
                raise ExportFailed(status)

            elif status == "Complete":
                return True

            time.sleep(POLL_INTERVAL)

        raise ExportFailed("Timed out")
