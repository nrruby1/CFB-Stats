from enum import Enum
import logging
from typing import Type
from pymongo.mongo_client import MongoClient
from pymongo.database import Database
from pymongo.collection import Collection
from pymongo.server_api import ServerApi

import config
from .model.cfb_model import CfbBaseModel
from .model.team import *
from .model.conference import *
from .model.venue import *

class Databases(Enum):
    extraction = "cfb_extraction"
    staging = "cfb_staging"
    production = "cfb_data"

class ExtractionCollections(Enum):
    team = "team"
    conference = "conference"
    venue = "venue"
    game = "game"
    game_team = "game_team"

cfb_models = {
    Conference, 
    Team, 
    TeamExt, 
    Venue
}

class DbConnection(MongoClient):
    def __init__(self, log: logging.Logger = logging.getLogger()):
        self.log = log
        super().__init__(config.db_uri, server_api=ServerApi('1'))

    def __del__(self):
        self.log.debug("MongoDb client closed")
        super().close()
        super().__del__()

    def get_cfb_database(self, db: Databases) -> Database:
        return self.get_database(db.value)
    
    def get_cfb_collection(self, db: Databases, model: ExtractionCollections | Type[CfbBaseModel]) -> Collection:
        if isinstance(model, ExtractionCollections):
            if db is not Databases.extraction:
                raise Exception("get_cfb_collection: Extraction collections can only be fetched from Extraction DB")
            return self.get_cfb_database(db)[model.value]
        
        if issubclass(model, CfbBaseModel):
            if db is not Databases.staging and db is not Databases.production:
                raise Exception("get_cfb_collection: Model collections can only be fetched from Staging or Extraction")
            return self.get_cfb_database(db)[model.model_id()]
        
        raise Exception("get_cfb_collection: Invalid 'model' argument")
    
    def get_cfb_repository(self, db: Databases, model: Type[CfbBaseModel]) -> AbstractRepository:
        if db is not Databases.staging and db is not Databases.production:
            raise Exception("get_cfb_repository: Database must either be Staging or Extraction")
        repo = model.model_repository()
        return repo(self.get_cfb_database(db))