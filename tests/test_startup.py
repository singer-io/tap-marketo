import unittest

import pendulum
import requests_mock

from tap_marketo import validate_state
from tap_marketo.sync import determine_replication_key

class TestValidateState(unittest.TestCase):
    def test_validate_state(self):

        mock_catalog = {
            'streams' : [
                {
                    "tap_stream_id": "activities_visit_webpage",
                    "stream": "activities_visit_webpage",
                    "key_properties": ["marketoGUID"],
                    "metadata" : [
                        {'breadcrumb': (),
                         'metadata': {'marketo.activity-id': 1,
                                      'selected' : True,
                                      'marketo.primary-attribute-name': 'webpage_id'}},
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
                },
                {
                    "tap_stream_id": "leads",
                    "stream": "leads",
                    "key_properties": ["marketoGUID"],
                    "metadata" : [
                        {'breadcrumb': (),
                         'metadata': {'marketo.activity-id': 1,
                                      'selected' : False,
                                      'marketo.primary-attribute-name': 'webpage_id'}},
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
            ]
        }

        mock_config = {
            'start_date' : "2019-09-09T00:00:00Z"
        }

        mock_state_1 = {
            'currently_syncing' : "activities_visit_webpage",
            'bookmarks': {
                "leads": {
                    determine_replication_key('leads') : '2019-09-08T00:00:00Z'
                }
            }
        }

        mock_state_2 = {
            'currently_syncing' : "leads",
            'bookmarks' : {
                "activities_visit_webpage" : {
                    determine_replication_key('activities_visit_webpage') : mock_config['start_date']
                },
                "leads": {
                    determine_replication_key('leads') : '2019-09-08T00:00:00Z'
                }
            }
        }

        expected_state_1 = {
            'currently_syncing' : "activities_visit_webpage",
            'bookmarks': {
                "activities_visit_webpage" : {
                    determine_replication_key('activities_visit_webpage') : mock_config['start_date']
                },
                "leads": {
                    determine_replication_key('leads') : '2019-09-08T00:00:00Z'
                }
            }
        }

        expected_state_2 = {
            'currently_syncing': None,
            'bookmarks': {
                "activities_visit_webpage" : {
                    determine_replication_key('activities_visit_webpage') : mock_config['start_date']
                },
                "leads": {
                    determine_replication_key('leads') : '2019-09-08T00:00:00Z'
                }
            }
        }

        self.assertDictEqual(validate_state(mock_config, mock_catalog, mock_state_1),
                             expected_state_1)

        self.assertDictEqual(validate_state(mock_config, mock_catalog, mock_state_2),
                             expected_state_2)
