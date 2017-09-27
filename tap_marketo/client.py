import datetime

import requests
import singer


LOGGER = singer.get_logger()


class ApiException(Exception):
    pass


class Client:
    def __init__(self, endpoint, identity, client_id, client_secret,
                 max_daily_calls=8000,
                 user_agent="Singer.io/tap-marketo"):

        if not endpoint.endswith("/"):
            endpoint += "/"

        if endpoint.endswith("rest/"):
            endpoint = endpoint[:-5]

        self.endpoint = endpoint
        self.identity = identity

        self.client_id = client_id
        self.client_secret = client_secret
        self.max_daily_calls = int(max_daily_calls)
        self.user_agent = user_agent

        self.token_expires = None
        self.access_token = None
        self.calls_today = 0

        self.refresh_token()
        self._session = requests.Session()
        self._session.headers = self.headers

    def get_url(self, url):
        return self.endpoint + url

    @property
    def headers(self):
        if not self.token_expires or self.token_expires <= datetime.datetime.utcnow():
            raise Exception("No valid token")

        rtn = {"Authorization": "Bearer {}".format(self.access_token)}
        if self.user_agent:
            rtn["User-Agent"] = self.user_agent

        return rtn

    def refresh_token(self):
        params = {
            "grant_type": "client_credentials",
            "client_id": self.client_id,
            "client_secret": self.client_secret,
        }
        LOGGER.info("Refreshing token")

        try:
            resp = requests.get(self.get_url("identity/oauth/token"), params=params)
            resp_time = datetime.datetime.utcnow()
        except requests.exceptions.ConnectionError:
            err = (
                "Connection error while refreshing token at %s. "
                "Identity must match `https://123-ABC-456.mktorest.com/identity`."
            ) % self.identity
            raise ApiException(err)

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
        self.token_expires = resp_time + datetime.timedelta(seconds=data['expires_in'] - 15)
        self._session.headers = self.headers
        LOGGER.info("Token valid until %s", self.token_expires)

    def _request(self, method, url, stream=False, **kwargs):
        req = requests.Request(method, url, **kwargs).prepare()
        LOGGER.info("%s: %s", method, req.url)
        resp = self._session.send(req, stream=stream)
        if resp.status_code >= 400:
            raise ApiException(resp)

        return resp

    def update_calls_today(self):
        data = self._request("GET", self.get_url("rest/v1/stats/usage.json")).json()
        self.calls_today = int(data["result"][0]["total"])
        LOGGER.info("Used %s of %s requests", self.calls_today, self.max_daily_calls)

    @singer.utils.ratelimit(100, 20)
    @singer.utils.backoff((requests.exceptions.RequestException), singer.utils.exception_is_4xx)
    def request(self, method, url, **kwargs):
        if not self.token_expires or self.token_expires <= datetime.datetime.utcnow():
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
                raise ApiException("API returned error: {}: {}".format(resp.status_code, resp.content))

            return resp
