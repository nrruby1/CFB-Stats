from pprint import pprint
from db.model.team import Team
from etl.etl_init import EtlInit, InitDataset
from db.model.conference import Conference, ConferenceRepository
from db.model.cfb_model import CfbBaseModel
from db.db_connection import DbConnection
from db.db_cleanup import *
from etl.etl import *
from etl.extraction_datasets import *
from logging import *

def run(db_client: DbConnection, cfbd_client: CfbdConnection):

    years = [2025]
    weeks = [1]
    classifications = ["fbs"]
    ext_ds = ExtractGamesDataSet(logging.getLogger(), year_list=years, class_list=classifications, week_list=weeks)
    ext_ds.cleanup(db_client)
    if ext_ds.extract(cfbd_client, db_client) :
        print("success")
    else:
        print("failed")

def test(db_client: DbConnection, entity: CfbBaseModel, query):
    repo = db_client.get_staging_repository(Team)
    result = repo.find_one_by(query(entity))
    if result is not None:
        print("found")
    else:
        # repo.save(entity)
        print("not found")



with DbConnection() as db_client, CfbdConnection() as cfbd_client:
    run(db_client, cfbd_client)
    print("Done")

# class TestClass():
#     def __init__(self):
#         print(self.test)

# TestClass()