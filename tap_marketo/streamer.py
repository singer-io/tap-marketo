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
        rtn = {
            "fields": self.fields,
            "format": "CSV",
        }

        if self.query:
            rtn["filter"] = self.query

        return rtn

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
    def _run_job(self, query, export_id):
        job = ExportJob(self.client, self.entity.get_fields(), query, export_id)
        job.start()

        self.state.set_export_id(job.export_id)
        singer.write_state(self.state.to_dict())

        for row in job:
            record = self.entity.format_values(row)
            if self.entity.record_is_new(record, self.state):
                yield record

        self.state.set_export_id(None)
        singer.write_state(self.state.to_dict())

    def _iter_corona(self):
        now_dt = datetime.datetime.utcnow()

        start_str = self.state.get_bookmark(self.entity)
        start_dt = singer.utils.strptime(start)

        while start_dt < now_dt:
            export_id = self.state.get_export_id(self.entity)
            query = self.entity.get_query(start_dt, self.client)
            self._run_job(query, export_id)
            start_dt = singer.utils.strptime(query[self.entity.replication_key]["endsAt"])

    def _iter_without_corona(self):
        export_id = self.state.get_export_id(self.entity)
        query = None
        self._run_job(query, export_id)

    def __iter__(self):
        if self.state.use_corona:
            iter_method = self._iter_corona
        else:
            iter_method = self._iter_without_corona

        for record in iter_method():
            yield record


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
