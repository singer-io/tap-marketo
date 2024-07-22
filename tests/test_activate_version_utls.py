import unittest
from tap_marketo.sync import get_activate_version, check_if_first_sync

class TestActivateversionUtils(unittest.TestCase):

    def test_first_sync_is_false_trueval(self):
        """
            Verify that first sync returns False if is_inital_sync_run is set to true
        """
        state = {
            "bookmarks": {
                "program_tags": {
                    "is_inital_sync_run": True,
                    "program_id": 1234 }
                    },
                "currently_syncing": "program_tags"
                }

        self.assertEqual(check_if_first_sync(state, "program_tags"), False)

    def test_first_sync_is_true_noneval(self):
        """
            Verify that first sync returns True if is_inital_sync_run is set to None
        """
        state = {
            "bookmarks": {
                "program_tags": {
                    "is_inital_sync_run": None,
                    "program_id": 1234 }
                    },
                "currently_syncing": "program_tags"
                }

        self.assertEqual(check_if_first_sync(state, "program_tags"), True)

    def test_first_sync_is_true_emptyval(self):
        """
            Verify that first sync returns True if is_inital_sync_run key is missing
        """
        state = {
            "bookmarks": {
                "program_tags": {
                    "program_id": 1234 }
                    },
                "currently_syncing": "program_tags"
                }

        self.assertEqual(check_if_first_sync(state, "program_tags"), True)

    def test_first_sync_is_true_falseval(self):
        """
            Verify that first sync returns False if is_inital_sync_run key is set to True
        """
        state = {
            "bookmarks": {
                "program_tags": {
                    "is_inital_sync_run": False,
                    "program_id": 1234 }
                    },
                "currently_syncing": "program_tags"
                }

        self.assertEqual(check_if_first_sync(state, "program_tags"), True)

    def test_get_activate_version_old(self):
        """
            Verify that activate version value is returnd if already exists
        """
        state = {
            "bookmarks": {
                "program_tags": {
                    "is_inital_sync_run": 1,
                    "active_version": 1721633019131,
                    "program_id": 1234 }
                    },
                "currently_syncing": "program_tags"
                }

        self.assertEqual(get_activate_version(state, "program_tags"), 1721633019131)

    def test_get_activate_version_does_not_exist(self):
        """
            Verify that activate version value is not returned for wrong stream
        """
        state = {
            "bookmarks": {
                "program_tags": {
                    "is_inital_sync_run": 1,
                    "active_version": 1721633019131,
                    "program_id": 1234 }
                    },
                "currently_syncing": "program_tags"
                }

        self.assertNotEqual(get_activate_version(state, "invalid_stream"), 1721633019131)
