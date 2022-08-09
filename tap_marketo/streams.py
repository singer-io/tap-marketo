from datetime import datetime
import csv
import json
import decimal
import pendulum

import singer
from singer import utils
from tap_marketo.client import ExportFailed, ApiQuotaExceeded
from singer import (metrics, bookmarks, metadata, Transformer)
from singer.transform import SchemaKey

LOGGER = singer.get_logger()
DATE_FORMAT = '%Y-%m-%dT%H:%M:%SZ'


def get_bookmark(state, stream_name, bookmark_key, start_date):
    """
    Return bookmark value if available in the state otherwise return start date
    """
    bookmark = bookmarks.get_bookmark(state, stream_name, bookmark_key, start_date)
    return bookmark

def get_schema(catalog, stream_id):
    """
    Return catalog of the specified stream.
    """
    stream_catalog = [cat for cat in catalog if cat['tap_stream_id'] == stream_id ][0]
    return stream_catalog


def write_bookmarks(stream, selected_streams, bookmark_value, state):
    stream_obj = STREAMS[stream]()
    # If the stream is selected, write the bookmark.
    if stream in selected_streams:
        singer.write_bookmark(state, stream_obj.tap_stream_id, stream_obj.replication_key, bookmark_value)


def new_transform(self, data, typ, schema, path):
    LOGGER.info(f'type {typ}')
    if self.pre_hook:
        data = self.pre_hook(data, typ, schema)

    if typ == "null":
        if data is None or data == "":
            return True, None
        else:
            return False, None

    elif schema.get("format") == "date-time":
        data = self._transform_datetime(data)
        if data is None:
            return False, None

        return True, data
    elif schema.get("format") == "singer.decimal":
        if data is None:
            return False, None

        if isinstance(data, (str, float, int)):
            try:
                return True, str(decimal.Decimal(str(data)))
            except:
                return False, None
        elif isinstance(data, decimal.Decimal):
            try:
                if data.is_snan():
                    return True, 'NaN'
                else:
                    return True, str(data)
            except:
                return False, None

        return False, None
    elif typ == "object":
        # Objects do not necessarily specify properties
        return self._transform_object(data,
                                        schema.get("properties", {}),
                                        path,
                                        schema.get(SchemaKey.pattern_properties))

    elif typ == "array":
        return self._transform_array(data, schema["items"], path)

    elif typ == "string":
        if data is not None:
            try:
                return True, str(data)
            except:
                return False, None
        else:
            return False, None

    elif typ == "integer":
        if isinstance(value, int):
            return True, value

        # Custom Marketo percent type fields can have decimals, so we drop them
        decimal_index = value.find('.')
        if decimal_index > 0:
            singer.log_warning("Dropping decimal from integer type. Original Value: %s", value)
            value = value[:decimal_index]
        return True, int(value)

    elif typ == "number":
        if isinstance(data, str):
            data = data.replace(",", "")

        try:
            return True, float(data)
        except:
            return False, None

    elif typ == "boolean":
        if isinstance(data, str):
            return True, False

        try:
            return True, bool(data)
        except:
            return False, None

    else:
        return False, None

# To cast the boolean values differently, overwriting this function of the
# Transformer class of the singer module
Transformer._transform = new_transform

class Stream:
    tap_stream_id = None
    replication_method = None
    replication_key = None
    key_properties = []
    path = None
    filter_param = False
    parent = None
    url = ''

class PaginatedStream(Stream):
    # http://developers.marketo.com/rest-api/endpoint-reference/lead-database-endpoint-reference/#!/Campaigns/getCampaignsUsingGET
    # http://developers.marketo.com/rest-api/endpoint-reference/lead-database-endpoint-reference/#!/Static_Lists/getListsUsingGET
    #
    # Campaigns and Static Lists are paginated with a max return of 300
    # items per page. There are no filters that can be used to only
    # return updated records.
    def sync_endpoint(self,
                        client,
                        state,
                        catalog,
                        start_date,
                        selected_stream_ids
                        ):

        stream_catalog = get_schema(catalog, self.tap_stream_id)
        bookmark = get_bookmark(state, self.tap_stream_id, self.replication_key, start_date)
        params = {"batchSize": 300}
        endpoint = "rest/v1/{}.json".format(self.tap_stream_id)

        # Paginated requests use paging tokens for retrieving the next page
        # of results. These tokens are stored in the state for resuming
        # syncs. If a paging token exists in state, use it.
        next_page_token = bookmarks.get_bookmark(state, self.tap_stream_id, "next_page_token")
        if next_page_token:
            params["nextPageToken"] = next_page_token

        # Keep querying pages of data until no next page token.
        record_count = 0
        job_started = pendulum.utcnow().isoformat()
        while True:
            data = client.request("GET", endpoint, endpoint_name=self.tap_stream_id, params=params)

            # Each row just needs the values formatted. If the record is
            # newer than the original start date, stream the record. Finally,
            # update the bookmark if newer than the existing bookmark.
            with singer.metrics.record_counter(self.tap_stream_id) as counter: 
                with singer.Transformer() as transformer:
                    extraction_time = singer.utils.now()
                    stream_metadata = singer.metadata.to_map(stream_catalog['metadata'])
                    for row in data["result"]:
                        # record = format_values(stream, row)
                        record = transformer.transform(row, stream_catalog['schema'], stream_metadata)
                        if record[self.replication_key] >= bookmark:
                            counter.increment()
                            singer.write_record(self.tap_stream_id, record, time_extracted=extraction_time)

                # No next page, results are exhausted.
                if "nextPageToken" not in data:
                    break

                # Store the next page token in state and continue.
                params["nextPageToken"] = data["nextPageToken"]
                state = bookmarks.write_bookmark(state, self.tap_stream_id, "next_page_token", data["nextPageToken"])
                singer.write_state(state)

        # Once all results are exhausted, unset the next page token bookmark
        # so the subsequent sync starts from the beginning.
        state = bookmarks.write_bookmark(state, self.tap_stream_id, "next_page_token", None)
        state = bookmarks.write_bookmark(state, self.tap_stream_id, self.replication_key, job_started)
        singer.write_state(state)
        return state, record_count

class Campaigns:
    pass

class ActivityTypes:
    pass

class Leads:
    pass

class Lists:
    pass

class Programs:
    pass

# Dictionary of the stream classes
STREAMS = {
    'campaigns': Campaigns,
    'activity_types': ActivityTypes,
    'leads': Leads,
    'lists': Lists,
    'programs': Programs
}
