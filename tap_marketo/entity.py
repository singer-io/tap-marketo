import datetime

import singer


MAX_EXPORT_DAYS = 30

# Users can opt out of lead-mutating activity types for their syncs.
# These IDs are the IDs of the activity types we ignore.
LEAD_ACTIVITY_TYPE_IDS = [
    12,  # new lead
    13,  # updated lead
]


def format_value(value, schema):
    if not isinstance(schema.type, list):
        field_type = [schema.type]
    else:
        field_type = schema.type

    if value == "":
        return None

    elif schema.format == "date-time":
        if len(value) == 10:
            value += "T00:00:00Z"

        return value

    elif "integer" in field_type:
        return int(value)

    elif "number" in field_type:
        return float(value)

    elif "boolean" in field_type:
        if isinstance(value, bool):
            return value

        return value.lower() == "true"

    return value


class Entity:
    def __init__(self, name, key_properties, replication_key, schema, ignore_lead_activities=False):
        self.name = name
        self.key_properties = key_properties
        self.replication_key = replication_key
        self.schema = schema
        self.ignore_lead_activities = ignore_lead_activities

    @classmethod
    def from_catalog_entry(cls, catalog_entry):
        return cls(
            name=catalog_entry.tap_stream_id,
            key_properties=catalog_entry.key_properties,
            replication_key=catalog_entry.replication_key,
            schema=catalog_entry.schema,
        )

    @property
    def endpoint(self):
        return "rest/v1/{}.json".format(self.name)

    def get_fields(self):
        return sorted([f for f, p in self.schema.properties.items() if p.selected])

    def format_values(self, row):
        record = {}
        for field_name, field_schema in self.schema.properties.items():
            if not field_schema.selected:
                continue

            record[field_name] = row.get(field_name)
        rtn = {}
        pass

    def get_bookmark(self, state):
        if not self.replication_key:
            return None

        return state.get_bookmark(self)

    def update_bookmark(self, record, state):
        if self.replication_key and record[self.replication_key] > state.get_bookmark(self):
            state.set_bookmark(self, record[self.replication_key])

    def get_query(self, start):
        raise NotImplemented()


class LeadEntity(Entity):
    def get_query(self, start_dt, client):
        end_dt = start_dt + datetime.timedelta(days=MAX_EXPORT_DAYS)
        return {
            self.replication_key: {
                "startAt": singer.utils.strftime(start_dt),
                "endAt": singer.utils.strftime(end_dt),
            }
        }


class ActivityEntity(Entity):
    def get_activity_type_ids(self, client):
        data = client.get("rest/v1/activities/types.json")
        return [row["id"] for row in data["result"]]

    def get_query(self, start_dt, client):
        end_dt = start_dt + datetime.timedelta(days=MAX_EXPORT_DAYS)
        rtn = {
            self.replication_key: {
                "startAt": singer.utils.strftime(start_dt),
                "endAt": singer.utils.strftime(end_dt),
            }
        }

        if self.ignore_lead_activities:
            activity_type_ids = self.get_activity_type_ids(client)
            for activity_type_id in LEAD_ACTIVITY_TYPE_IDS:
                activity_type_ids.remove(activity_type_id)

            rtn["activityTypeIds"] = activity_type_ids

        return rtn
