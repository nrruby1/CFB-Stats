from enum import Enum
import logging
from typing import Type
from pymongo.mongo_client import MongoClient
from pymongo.database import Database
from pymongo.collection import Collection
from pymongo.server_api import ServerApi
from pydantic_mongo import AbstractRepository

import config
from db.model.cfb_model import CfbBaseModel
from db.model.conference import Conference
from db.model.game import Game, GameTeamStats
from db.model.team import Team, TeamExt
from db.model.venue import Venue

log = logging.getLogger("CfbStats.db")


class Databases(Enum):
    extraction = "cfb_extraction"
    staging = "cfb_staging"
    production = "cfb_data"


class ExtractionCollections(Enum):
    conference = "conference"
    game = "game"
    game_team_stats = "game_team_stats"
    team = "team"
    venue = "venue"


cfb_models = {Conference, Game, GameTeamStats, Team, TeamExt, Venue}


class DbConnection(MongoClient):
    def __init__(self, test_mode: bool = False):
        super().__init__(config.db_uri, server_api=ServerApi("1"))
        self.test_mode = test_mode

    def __del__(self):
        log.debug("MongoDb client closed")
        super().close()
        super().__del__()

    def get_cfb_database(self, db: Databases) -> Database:
        if self.test_mode:
            return self.get_database("test_" + db.value)
        else:
            return self.get_database(db.value)

    def get_cfb_collection(
        self, db: Databases, model: ExtractionCollections | Type[CfbBaseModel]
    ) -> Collection:
        if isinstance(model, ExtractionCollections):
            if db is not Databases.extraction:
                raise Exception(
                    "get_cfb_collection: Extraction collections can only be fetched from Extraction DB"
                )
            return self.get_cfb_database(db)[model.value]

        if issubclass(model, CfbBaseModel):
            if db is not Databases.staging and db is not Databases.production:
                raise Exception(
                    "get_cfb_collection: Model collections can only be fetched from Staging or Extraction"
                )
            return self.get_cfb_database(db)[model.model_id()]

        raise Exception("get_cfb_collection: Invalid 'model' argument")

    def get_cfb_repository(
        self, db: Databases, model: Type[CfbBaseModel]
    ) -> AbstractRepository:
        if db is not Databases.staging and db is not Databases.production:
            raise Exception(
                "get_cfb_repository: Database must either be Staging or Production"
            )
        repo = model.model_repository()
        return repo(self.get_cfb_database(db))

    def get_collection_namespace(
        self, db: Databases, model: ExtractionCollections | Type[CfbBaseModel]
    ) -> str:

        collection_name = ""
        if isinstance(model, ExtractionCollections):
            if db is not Databases.extraction:
                raise Exception(
                    "get_cfb_collection: Extraction collections can only be fetched from Extraction DB"
                )
            collection_name = model.value

        elif issubclass(model, CfbBaseModel):
            if db is not Databases.staging and db is not Databases.production:
                raise Exception(
                    "get_cfb_collection: Model collections can only be fetched from Staging or Production"
                )
            collection_name = model.model_id()

        else:
            raise Exception("get_collection_namespace: Invalid 'model' argument")

        if self.test_mode:
            return f"test_{db.value}.{collection_name}"
        else:
            return f"{db.value}.{collection_name}"
