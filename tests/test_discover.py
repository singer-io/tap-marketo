import unittest

import pendulum
import requests_mock
from singer import metadata

from tap_marketo.client import Client
from tap_marketo.discover import *
from tap_marketo.sync import determine_replication_key


class TestDiscover(unittest.TestCase):
    def test_get_activity_type_stream(self):
        activity = {
            "id": 1,
            "name": "Visit Webpage",
            "description": "User visits a web page",
            "primaryAttribute": {
                "name": "Webpage ID",
                "dataType": "integer",
            },
            "attributes": [
                {
                    "name": "Client IP Address",
                    "dataType": "string",
                },
                {
                    "name": "Query Parameters",
                    "dataType": "string",
                },
            ],
        }

        stream = {
            "tap_stream_id": "activities_visit_webpage",
            "stream": "activities_visit_webpage",
            "key_properties": ["marketoGUID"],
            "metadata" : [
                {'breadcrumb': (),
                 'metadata': {'table-key-properties': ['marketoGUID'],
                              'marketo.activity-id': 1,
                              'marketo.primary-attribute-name': 'webpage_id',
                              'forced-replication-method': 'FULL_TABLE'}},
                {
                    "metadata" : {
                        "inclusion": "automatic"
                    },
                    "breadcrumb" : ("properties", 'marketoGUID')
                },
                {
                    "metadata" : {
                        "inclusion": "automatic"
                    },
                    "breadcrumb" : ("properties", 'leadId')
                },
                {
                    "metadata" : {
                        "inclusion": "automatic"
                    },
                    "breadcrumb" : ("properties", 'activityDate')
                },
                {
                    "metadata" : {
                        "inclusion": "automatic"
                    },
                    "breadcrumb" : ("properties", 'activityTypeId')
                },
                {
                    "metadata" : {
                        "inclusion": "available"
                    },
                    "breadcrumb" : ("properties", 'campaignId')
                },
                {
                    "metadata" : {
                        "inclusion": "automatic"
                    },
                    "breadcrumb" : ("properties", 'primary_attribute_name')
                },
                {
                    "metadata" : {
                        "inclusion": "automatic"
                    },
                    "breadcrumb" : ("properties", 'primary_attribute_value_id')
                },
                {
                    "metadata" : {
                        "inclusion": "automatic"
                    },
                    "breadcrumb" : ("properties", 'primary_attribute_value')
                },
                {
                    "metadata" : {
                        "inclusion": "available"
                    },
                    "breadcrumb" : ("properties", 'client_ip_address')
                },
                {
                    "metadata" : {
                        "inclusion": "available"
                    },
                    "breadcrumb" : ("properties", 'query_parameters')
                },
            ],
            "schema": {
                "type": "object",
                "additionalProperties": False,
                "properties": {
                    "marketoGUID": {
                        "type": ["null", "string"],
                    },
                    "leadId": {
                        "type": ["null", "integer"],
                    },
                    "activityDate": {
                        "type": ["null", "string"],
                        "format": "date-time",
                    },
                    "activityTypeId": {
                        "type": ["null", "integer"],
                    },
                    "campaignId": {
                        "type": ["null", "integer"],
                    },
                    "primary_attribute_name": {
                        "type": ["null", "string"],
                    },
                    "primary_attribute_value_id": {
                        "type": ["null", "string"],
                    },
                    "primary_attribute_value": {
                        "type": ["null", "string"],
                    },
                    "client_ip_address": {
                        "type": ["string", "null"],
                    },
                    "query_parameters": {
                        "type": ["string", "null"],
                    },
                },
            },
        }
        result = get_activity_type_stream(activity)
        result_metadata = result.pop("metadata")
        stream_metadata = stream.pop('metadata')
        automatic_count = 0
        for mdata in result_metadata:
            if mdata['metadata'].get('inclusion') == 'automatic':
                automatic_count += 1
        self.assertDictEqual(stream, result)
        self.assertEqual(sorted(result_metadata, key=lambda x: x['breadcrumb']),
                         sorted(stream_metadata, key=lambda x: x['breadcrumb']))
        self.assertEqual(11, len(result_metadata))
        self.assertEqual(7,automatic_count)

    def test_discover_leads(self):
        client = Client("123-ABC-456", "id", "secret")
        client.token_expires = pendulum.utcnow().add(days=1)
        client.calls_today = 1
        data = {
            "success": True,
            "result": [
                {"displayName": "id", "dataType": "string", "rest": {"name": "id"}},
                {"displayName": "foo", "dataType": "string", "rest": {"name": "foo"}},
                {"displayName": "bar", "dataType": "string", "soap": {"name": "bar"}},
            ],
        }

        stream = {
            "tap_stream_id": "leads",
            "stream": "leads",
            "key_properties": ["id"],
            "schema": {
                "type": "object",
                "additionalProperties": False,
                "properties": {
                    "id": {
                        "type": "string",
                    },
                    "foo": {
                        "type": ["string", "null"],
                    },
                },
            },
        }

        with requests_mock.Mocker(real_http=True) as mock:
            mock.register_uri("GET", client.get_url("rest/v1/leads/describe.json"), json=data)
            self.maxDiff = None
            result = discover_leads(client)
            metadata = result.pop("metadata")
            automatic_count = 0
            root_metadata = None
            for mdata in metadata:
                if mdata.get('metadata', {}).get('inclusion') == 'automatic':
                    automatic_count += 1
                if mdata['breadcrumb'] == ():
                    root_metadata = mdata['metadata']
            
            self.assertDictEqual(stream, result)
            self.assertEqual(3,len(metadata))
            self.assertEqual(1,automatic_count)
            # Test new replication metadata
            self.assertEqual(root_metadata['forced-replication-method'], 'INCREMENTAL')
            self.assertEqual(root_metadata['valid-replication-keys'], 'updatedAt')

    def test_discover_catalog_campaigns(self):
        result = discover_catalog("campaigns", CAMPAIGNS_AUTOMATIC_INCLUSION)
        metadata = result["metadata"]
        
        # Find root metadata
        root_metadata = None
        for mdata in metadata:
            if mdata['breadcrumb'] == ():
                root_metadata = mdata['metadata']
                break
        
        # Test basic properties
        self.assertEqual(result["tap_stream_id"], "campaigns")
        self.assertEqual(result["key_properties"], ["id"])
        
        # Test new replication metadata
        self.assertEqual(root_metadata['forced-replication-method'], 'INCREMENTAL')
        self.assertEqual(root_metadata['valid-replication-keys'], 'updatedAt')
        self.assertEqual(root_metadata['table-key-properties'], ['id'])

    def test_discover_catalog_activity_types(self):
        result = discover_catalog("activity_types", ACTIVITY_TYPES_AUTOMATIC_INCLUSION, unsupported=ACTIVITY_TYPES_UNSUPPORTED)
        metadata = result["metadata"]
        
        # Find root metadata
        root_metadata = None
        for mdata in metadata:
            if mdata['breadcrumb'] == ():
                root_metadata = mdata['metadata']
                break
        
        # Test basic properties
        self.assertEqual(result["tap_stream_id"], "activity_types")
        self.assertEqual(result["key_properties"], ["id"])
        
        # Test new replication metadata - activity_types should be FULL_TABLE with no valid replication keys
        self.assertEqual(root_metadata['forced-replication-method'], 'FULL_TABLE')
        self.assertNotIn('valid-replication-keys', root_metadata)
        self.assertEqual(root_metadata['table-key-properties'], ['id'])

    def test_set_replication_metadata(self):
        # Test with no valid replication keys (FULL_TABLE)
        mdata = metadata.new()
        result = set_replication_metadata(mdata, None)
        result_list = metadata.to_list(result)
        result_dict = metadata.to_map(result_list)
        
        self.assertEqual(result_dict[()]['forced-replication-method'], 'FULL_TABLE')
        self.assertNotIn('valid-replication-keys', result_dict[()])
        
        # Test with valid replication keys (INCREMENTAL)
        mdata = metadata.new()
        result = set_replication_metadata(mdata, 'updatedAt')
        result_list = metadata.to_list(result)
        result_dict = metadata.to_map(result_list)
        
        self.assertEqual(result_dict[()]['forced-replication-method'], 'INCREMENTAL')
        self.assertEqual(result_dict[()]['valid-replication-keys'], 'updatedAt')

    def test_determine_replication_key(self):
        # Test activity streams
        self.assertEqual(determine_replication_key('activities_visit_webpage'), 'activityDate')
        self.assertEqual(determine_replication_key('activities_email_sent'), 'activityDate')
        
        # Test other streams
        self.assertEqual(determine_replication_key('leads'), 'updatedAt')
        self.assertEqual(determine_replication_key('campaigns'), 'updatedAt')
        self.assertEqual(determine_replication_key('lists'), 'updatedAt')
        self.assertEqual(determine_replication_key('programs'), 'updatedAt')
        
        # Test streams with no replication key
        self.assertIsNone(determine_replication_key('activity_types'))
        self.assertIsNone(determine_replication_key('unknown_stream'))

    def test_discover_catalog_lists(self):
        result = discover_catalog("lists", LISTS_AUTOMATIC_INCLUSION)
        metadata = result["metadata"]
        
        # Find root metadata
        root_metadata = None
        for mdata in metadata:
            if mdata['breadcrumb'] == ():
                root_metadata = mdata['metadata']
                break
        
        # Test basic properties
        self.assertEqual(result["tap_stream_id"], "lists")
        self.assertEqual(result["key_properties"], ["id"])
        
        # Test new replication metadata
        self.assertEqual(root_metadata['forced-replication-method'], 'INCREMENTAL')
        self.assertEqual(root_metadata['valid-replication-keys'], 'updatedAt')
        self.assertEqual(root_metadata['table-key-properties'], ['id'])

    def test_discover_catalog_programs(self):
        result = discover_catalog("programs", PROGRAMS_AUTOMATIC_INCLUSION)
        metadata = result["metadata"]
        
        # Find root metadata
        root_metadata = None
        for mdata in metadata:
            if mdata['breadcrumb'] == ():
                root_metadata = mdata['metadata']
                break
        
        # Test basic properties
        self.assertEqual(result["tap_stream_id"], "programs")
        self.assertEqual(result["key_properties"], ["id"])
        
        # Test new replication metadata
        self.assertEqual(root_metadata['forced-replication-method'], 'INCREMENTAL')
        self.assertEqual(root_metadata['valid-replication-keys'], 'updatedAt')
        self.assertEqual(root_metadata['table-key-properties'], ['id'])
