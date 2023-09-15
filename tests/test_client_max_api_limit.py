import unittest
from tap_marketo.client import Client, MAX_DAILY_CALLS

class TestmaxdailycallsConfig(unittest.TestCase):
    
    def test_maxdailycalls_default_no_val(self):
        """
            Verify that max_daily_calls is default if no value is passed
        """
        # Initialize Client object
        client = Client(**{'endpoint': '123-ABC-789','client_id':'ABC-123','client_secret':'123-QRT'})
        
        self.assertEqual(client.max_daily_calls, MAX_DAILY_CALLS)
    
    def test_maxdailycalls_default_empty_str(self):
        """
            Verify that max_daily_calls is default if empty string value is passed
            Verify no Exception is raised for typecasting error between str to num
        """
        # Initialize Client object
        client = Client(**{'endpoint': '123-ABC-789','client_id':'ABC-123','client_secret':'123-QRT','max_daily_calls':''})
        
        self.assertEqual(client.max_daily_calls, MAX_DAILY_CALLS)

    def test_maxdailycalls_default_bool_false_val(self):
        """
            Verify that max_daily_calls is default if bool value is passed
        """
        # Initialize Client object
        client = Client(**{'endpoint': '123-ABC-789','client_id':'ABC-123','client_secret':'123-QRT','max_daily_calls':False})
        self.assertEqual(client.max_daily_calls ,MAX_DAILY_CALLS)

    def test_maxdailycalls_bool_true_val(self):
        """
            Verify that max_daily_calls is default if bool value is passed
        """
        # Initialize Client object
        params = {'endpoint': '123-ABC-789','client_id':'ABC-123','client_secret':'123-QRT','max_daily_calls':True}
        with self.assertRaises(ValueError):
            try:
                Client(**params)
            except Exception as err:
                self.assertEqual(str(err),"Limit Cannot be Negative or Zero")
                raise err

    def test_maxdailycalls_default_zero_val(self):
        """
            Verify that api_limit is default if 0 value is passed
        """
        # Initialize Client object
        client = Client(**{'endpoint': '123-ABC-789','client_id':'ABC-123','client_secret':'123-QRT','max_daily_calls':0})
        self.assertEqual(client.max_daily_calls, MAX_DAILY_CALLS)

    def test_maxdailycalls_default_none_val(self):
        """
            Verify that api_limit is default if None value is passed
        """
        # Initialize Client object
        client = Client(**{'endpoint': '123-ABC-789','client_id':'ABC-123','client_secret':'123-QRT','max_daily_calls':None})
        self.assertEqual(client.max_daily_calls, MAX_DAILY_CALLS)

    def test_maxdailycalls_enabled_num_val(self):
        """
            Verify that api_limit is set appropriately if num value is passed
        """
        # Initialize Client object
        client = Client(**{'endpoint': '123-ABC-789','client_id':'ABC-123','client_secret':'123-QRT','max_daily_calls':3})

        self.assertEqual(client.max_daily_calls, 3)

    def test_maxdailycalls_failed_comma_val(self):
        """
            Verify that exception is raised if invalid input value is passed
        """
        params = {'endpoint': '123-ABC-789','client_id':'ABC-123','client_secret':'123-QRT','max_daily_calls':'30,000'}
        # Initialize Client object
        with self.assertRaises(ValueError):
            Client(**params)

    def test_maxdailycalls_failed_decimal_val(self):
        """
            Verify that api_limit is set appropriately if num value is passed
        """
        # Initialize Client object
        params = {'endpoint': '123-ABC-789','client_id':'ABC-123','client_secret':'123-QRT','max_daily_calls':'3700.15'}
        with self.assertRaises(ValueError):
            Client(**params)

    def test_maxdailycalls_failed_negative_val(self):
        """
            Verify that api_limit is set appropriately if num value is passed
        """
        # Initialize Client object
        params = {'endpoint': '123-ABC-789','client_id':'ABC-123','client_secret':'123-QRT','max_daily_calls':'-522'}
        with self.assertRaises(ValueError):
            try:
                Client(**params)
            except Exception as err:
                self.assertEqual(str(err),"Limit Cannot be Negative or Zero")
                raise err

    def test_maxdailycalls_default_str_zero_val(self):
        """
            Verify that api_limit is default if "0" value is passed
        """
        # Initialize Client object
        params = {'endpoint': '123-ABC-789','client_id':'ABC-123','client_secret':'123-QRT','max_daily_calls':'0'}
        with self.assertRaises(ValueError):
            try:
                Client(**params)
            except Exception as err:
                self.assertEqual(str(err),"Limit Cannot be Negative or Zero")
                raise err
