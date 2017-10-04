import time

import pendulum
import requests
import singer


# By default, jobs will run for 30 minutes and be polled every 3 minutes.
JOB_TIMEOUT = 60 * 30
POLL_INTERVAL = 60 * 3

# If Corona is not supported, an error "1035" will be returned by the API.
# http://developers.marketo.com/rest-api/bulk-extract/bulk-lead-extract/#filters
NO_CORONA_CODE = "1035"

LOGGER = singer.get_logger()


class ApiException(Exception):
    """Indicates an error occured communicating with the Marketo API."""
    pass


class ExportFailed(Exception):
    """Indicates an error occured while attempting a bulk export."""
    pass


class Client:
    def __init__(self, domain, client_id, client_secret,
                 max_daily_calls=8000, user_agent="Singer.io/tap-marketo"
                 job_timeout=JOB_TIMEOUT, poll_interval=POLL_INTERVAL):

        self.domain = domain
        self.client_id = client_id
        self.client_secret = client_secret
        self.max_daily_calls = int(max_daily_calls)
        self.user_agent = user_agent
        self.job_timeout = job_timeout
        self.poll_interval = poll_interval

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
    def use_corona(self):
        if not hasattr(self, "_use_corona"):
            self._use_corona = self.test_corona()
        return self._use_corona

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

    @singer.utils.backoff((requests.exceptions.RequestException), singer.utils.exception_is_4xx)
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
            raise ApiException("Error refreshing token [%s]: %s", resp.status_code, resp.content)

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

    @singer.utils.ratelimit(100, 20)
    @singer.utils.backoff((requests.exceptions.RequestException), singer.utils.exception_is_4xx)
    def _request(self, method, url, stream=False, **kwargs):
        headers = kwargs.pop("headers", {})
        headers.update(self.headers)
        req = requests.Request(method, url, headers=headers, **kwargs).prepare()
        LOGGER.info("%s: %s", method, req.url)
        with singer.metrics.http_request_timer(url):
            resp = self._session.send(req, stream=stream)

        resp.raise_for_status()
        return resp

    def update_calls_today(self):
        data = self._request("GET", self.get_url("rest/v1/stats/usage.json")).json()
        if "result" not in data:
            raise ApiException(data)

        self.calls_today = int(data["result"][0]["total"])
        LOGGER.info("Used %s of %s requests", self.calls_today, self.max_daily_calls)

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
        timeout_time = pendulum.utcnow().add(seconds=self.job_timeout)
        while pendulum.utcnow() < timeout_time:
            endpoint = self.get_bulk_endpoint(stream_type, "status", export_id)
            status = self.request("GET", endpoint)["result"][0]["status"]

            if status == "Created":
                self.request("POST", self.get_bulk_endpoint("activities", "enqueue", export_id))

            elif status in ["Cancelled", "Failed"]:
                raise ExportFailed(status)

            elif status == "Complete":
                return True

            time.sleep(self.poll_interval)

        raise ExportFailed("Timed out")

    def test_corona(self):
        # Corona allows us to do bulk queries for Leads using updatedAt as a filter.
        # Clients without Corona (should only be clients with < 50,000 Leads) must
        # do a full Leads bulk export every sync.
        LOGGER.info("Testing for Corona support")
        start_pen = pendulum.utcnow().subtract(days=1).replace(microsecond=0)
        end_pen = start_pen.add(seconds=1)
        payload = {
            "format": "CSV",
            "fields": ["id"],
            "filter": {
                "updatedAt": {
                    "startAt": start_pen.isoformat(),
                    "endAt": end_pen.isoformat(),
                },
            },
        }
        endpoint = self.get_bulk_endpoint("leads", "create")
        data = self.request("POST", endpoint, json=payload)
        err_codes = set(err["code"] for err in data.get("errors", []))
        if NO_CORONA_CODE in err_codes:
            LOGGER.info("Corona not supported.")
            return False
        else:
            LOGGER.info("Corona is supported.")
            endpoint = self.get_bulk_endpoint("leads", "cancel", data["exportId"])
            self.request("POST", endpoint)
            return True
