import logging
from typing import Type
from pymongo.collection import Collection

from db import db_connection
from db.model.cfb_model import CfbBaseModel
from db.model.game import Game, GameTeamStats
from db.model.conference import Conference
from db.model.team import Team, TeamExt
from db.model.venue import Venue

log = logging.getLogger('CfbStats.db.scripts')

def setup_indexes(db_client: db_connection.DbConnection = db_connection.DbConnection()):
    log.info("Starting index setup")

    def get_collection_and_clean_indexes(model: Type[CfbBaseModel]):
        coll: Collection = db_client.get_cfb_collection(db_connection.Databases.production, model)
        coll.drop_indexes()
        log.debug(f"Dropped indexes for collection: {coll.name.capitalize()}")
        return coll

    # Conference
    conf_coll = get_collection_and_clean_indexes(model=Conference)
    conf_coll.create_index("conference_id", unique=True)
    conf_coll.create_index("name", unique=True)
    log.debug("Created 2 indexes for collection: Conference")

    # Team
    team_coll = get_collection_and_clean_indexes(model=Team)
    team_coll.create_index([("year"), ("team_id")], unique=True)
    team_coll.create_index([("year"), ("school")], unique=True)
    log.debug("Created 2 indexes for collection: Team")

    # TeamExt
    team_ext_coll = get_collection_and_clean_indexes(model=TeamExt)
    team_ext_coll.create_index([("year"), ("team_id")], unique=True)
    log.debug("Created 1 indexes for collection: TeamExt")

    # Venue
    venue_coll = get_collection_and_clean_indexes(model=Venue)
    venue_coll.create_index("venue_id", unique=True)
    log.debug("Created 1 indexes for collection: Venue")

    # Game
    game_coll = get_collection_and_clean_indexes(model=Game)
    game_coll.create_index("game_id", unique=True)
    game_coll.create_index([("year"), ("week"), ("season_type")])
    game_coll.create_index([("season"), ("home_id"), ("away_id")])
    log.debug("Created 2 indexes for collection: Game")

    # GameTeamStats
    game_team_stats_coll = get_collection_and_clean_indexes(model=GameTeamStats)
    game_team_stats_coll.create_index("game_id")
    game_team_stats_coll.create_index([("game_id"), ("team_id")], unique=True)
    log.debug("Created 1 indexes for collection: GameTeamStats")

    log.info("Index setup completed")