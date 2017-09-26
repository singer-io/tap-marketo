import datetime
import time

import singer


DEFAULT_POLL_INTERVAL = 60
DEFAULT_JOB_TIMEOUT = 3600

LOGGER = singer.get_logger()


class ExportFailed(Exception):
    pass


class ExportJob:
    def __init__(self, client, fields, query, export_id=None,
                 poll_interval=DEFAULT_POLL_INTERVAL,
                 timeout=DEFAULT_JOB_TIMEOUT):
        self.client = client
        self.fields = fields
        self.query = query
        self.export_id = export_id

        self.poll_interval = poll_interval
        self.timeout = timeout

    @property
    def payload(self):
        return {
            "fields": self.fields,
            "format": "CSV",
            "filter": self.query,
        }

    def start(self):
        data = self.client.request("POST", "bulk/v1/leads/export/create.json", json=self.payload)
        self.export_id = data["result"][0]["exportId"]
        self.client.request("POST", "bulk/v1/leads/export/{}/enqueue.json".format(self.export_id))
        return self.export_id

    def get_status(self):
        return self.client.request("GET", "bulk/v1/leads/export/{}/status.json".format(self.export_id))["result"][0]["status"]

    def stream_lines(self):
        return self.client.request("GET", "bulk/v1/leads/export/{}/file.json".format(self.export_id), stream=True).iter_lines()

    def __iter__(self):
        if not self.export_id:
            self.start()

        end = datetime.datetime.utcnow() + datetime.timedelta(seconds=self.timeout)
        while datetime.datetime.utcnow() < end:
            if self.poll() == "Complete":
                return self.stream_lines()

            time.sleep(self.poll_interval)

        raise ExportFailed("Export timed out")


class Streamer:
    def __init__(self, entity, client, state):
        self.entity = entity
        self.client = client
        self.state = state

    def __iter__(self):
        raise NotImplemented("Must be implemented in subclass")


class BulkStreamer:
    def __iter__(self):
        now_dt = datetime.datetime.utcnow()

        start_str = self.state.get_bookmark(self.entity)
        start_dt = singer.utils.strptime(start)

        export_id = self.state.get_export_id(self.entity)
        while start_dt < now_dt:
            query = self.entity.get_query(start_dt, self.client)
            job = ExportJob(self.client, self.entity.get_fields(), query, export_id)
            job.start()
            self.state.set_export_id(job.export_id)

            for row in job:
                record = self.entity.format_values(row)
                if self.entity.record_is_new(record, self.state):
                    yield record

            self.state.set_export_id(None)
            start_dt = singer.utils.strptime(query[self.entity.replication_key]["endsAt"])


class RestStreamer:
    def __iter__(self):
        params = {"_method": "GET"}
        data = {"fields": self.entity.get_fields()}

        while True:
            data = self.client.request("POST", self.entity.endpoint, params=params, data=data)
            if "result" not in data:
                break

            for row in data["result"]:
                record = self.entity.format_values(row)
                if self.entity.record_is_new(record, self.state):
                    yield record

            if data.get('moreResult', False):
                params['nextPageToken'] = data['nextPageToken']
