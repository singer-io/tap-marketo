import unittest
from datetime import datetime,timedelta
from tap_marketo.client import Client, MAX_DAILY_CALLS

class TestDateWindowConfig(unittest.TestCase):
    
    def test_datewindow_disabled_no_val(self):
        """
            Verify that daily_calls_limit is default if no value is passed
        """
        # Initialize Client object
        client = Client(**{'endpoint': "123-ABC-789",'client_id':'ABC-123','client_secret':'123-QRT'})
        
        self.assertEqual(client.max_daily_calls, MAX_DAILY_CALLS)
    
    def test_datewindow_disabled_empty_str(self):
        """
            Verify that daily_calls_limit is default if empty string value is passed
            Verify no Exception is raised for typecasting error between str to num
        """
        # Initialize Client object
        client = Client(**{'endpoint': "123-ABC-789",'client_id':'ABC-123','client_secret':'123-QRT',"max_daily_calls":""})
        
        self.assertEqual(client.max_daily_calls, MAX_DAILY_CALLS)

    def test_datewindow_disabled_bool_val(self):
        """
            Verify that daily_calls_limit is default if bool value is passed
        """
        # Initialize Client object
        client = Client(**{'endpoint': "123-ABC-789",'client_id':'ABC-123','client_secret':'123-QRT','max_daily_calls':False})
        self.assertEqual(client.max_daily_calls ,MAX_DAILY_CALLS)

    def test_datewindow_disabled_num_val(self):
        """
            Verify that api_limit is 0 if 0 value is passed
        """
        # Initialize Client object
        client = Client(**{'endpoint': "123-ABC-789",'client_id':'ABC-123','client_secret':'123-QRT',"max_daily_calls":0})
        self.assertEqual(client.max_daily_calls, MAX_DAILY_CALLS)

    def test_datewindow_disabled_none_val(self):
        """
            Verify that api_limit is default if None value is passed
        """
        # Initialize Client object
        client = Client(**{'endpoint': "123-ABC-789",'client_id':'ABC-123','client_secret':'123-QRT',"max_daily_calls":None})
        self.assertEqual(client.max_daily_calls, MAX_DAILY_CALLS)

    def test_datewindow_enabled_num_val(self):
        """
            Verify that api_limit is set appropriately if num value is passed
        """
        # Initialize Client object
        client = Client(**{'endpoint': "123-ABC-789",'client_id':'ABC-123','client_secret':'123-QRT',"max_daily_calls":3})

        self.assertEqual(client.max_daily_calls, 3)