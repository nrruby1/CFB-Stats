import unittest
from Lib.db.db_connection import DbConnection

class DbTest(unittest.TestCase):

    def test_get_databases(self):
        with DbConnection() as client:
            try:
                db = client.get_extraction_database()
            except Exception as e:
                print(e)
                self.fail("Test failed: test_get_cfb_databases")
            else:
                self.assertTrue(db != None, "Test failed: test_get_cfb_databases")

    def test_get_collections(self):
        with DbConnection() as client:
            try:
                coll1 = client.get_extract_team_collection()
                coll2 = client.get_extract_conference_collection()
            except Exception as e:
                print(e)
                self.fail("Test failed: test_get_teams_2025_collection")
            else:
                self.assertIsNotNone(coll1, "Test failed: test_get_collections - extract_team")
                self.assertIsNotNone(coll2, "Test failed: test_get_collections - extract_conference")