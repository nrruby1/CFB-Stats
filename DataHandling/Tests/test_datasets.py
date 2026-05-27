import pytest
from pymongo.results import ClientBulkWriteResult

from db.db_connection import *
from db.model.game import SeasonType

from etl.datasets.game_dataset import GameDataset
from etl.datasets.game_stats_dataset import GameStatsDataset
from etl.datasets.team_dataset import TeamDataset

log = logging.getLogger("CfbStats.Tests.test_datasets")

pytestmark = pytest.mark.usefixtures(
    "cleanup_db", "setup_extract_data", "cleanup_staging"
)


def test_game_dataset(db_client: DbConnection, model_checker, subtests):
    ds = GameDataset(
        years=[2025],
        classifications=["fbs"],
        weeks=list(range(1, 17)),
        season_types=[SeasonType.REGULAR],
    )

    # Test the transformation and loading into staging
    ops = []
    result = ds.transform(db_client, ops)
    assert result == True

    result: ClientBulkWriteResult = db_client.bulk_write(ops)
    assert result.inserted_count == len(ops)

    ext_games = list(
        db_client.get_cfb_collection(
            Databases.extraction, ExtractionCollections.game
        ).find({})
    )
    ext_venues = list(
        db_client.get_cfb_collection(
            Databases.extraction, ExtractionCollections.venue
        ).find({})
    )
    stg_games = list(db_client.get_cfb_collection(Databases.staging, Game).find({}))
    stg_venues = list(db_client.get_cfb_collection(Databases.staging, Venue).find({}))

    stg_game_ids = [game["game_id"] for game in stg_games]
    stg_venue_ids = [venue["venue_id"] for venue in stg_venues]
    stg_game_ids_unique = set(stg_game_ids)
    stg_venue_ids_unique = set(stg_venue_ids)

    ext_game_ids = {game["id"] for game in ext_games}
    stg_game_venue_ids = {game["venue_id"] for game in stg_games}

    # Test the appropriate entities are being loaded
    assert len(stg_game_ids) > 0
    assert len(stg_venue_ids) > 0

    assert ext_game_ids == stg_game_ids_unique
    assert stg_game_venue_ids == stg_venue_ids_unique

    # Test there are no duplicates
    assert len(stg_game_ids) == len(stg_game_ids_unique)
    assert len(stg_venue_ids) == len(stg_venue_ids_unique)

    # Test that the loaded entities are valid
    model_checker(Game, stg_games)
    model_checker(Venue, stg_venues)


def test_game_stats_dataset(db_client: DbConnection, model_checker, subtests):
    ds = GameStatsDataset(
        years=[2025],
        classifications=["fbs"],
        weeks=list(range(1, 17)),
        season_types=[SeasonType.REGULAR],
    )

    # Test the transformation and loading into staging
    ops = []
    result = ds.transform(db_client, ops)
    assert result == True

    result: ClientBulkWriteResult = db_client.bulk_write(ops)
    assert result.inserted_count == len(ops)

    ext_game_team_stats = list(
        db_client.get_cfb_collection(
            Databases.extraction, ExtractionCollections.game_team_stats
        ).find({})
    )
    stg_game_stats = list(
        db_client.get_cfb_collection(Databases.staging, GameTeamStats).find({})
    )

    stg_game_stat_ids = [
        str(stat["game_id"]) + "_" + str(stat["team_id"]) for stat in stg_game_stats
    ]
    stg_game_stat_ids_unique = set(stg_game_stat_ids)

    ext_game_stat_ids = {
        str(stat["id"]) + "_" + str(team["teamId"])
        for stat in ext_game_team_stats
        for team in stat["teams"]
    }

    # Test the appropriate entities are being loaded
    assert len(stg_game_stat_ids) > 0

    assert ext_game_stat_ids == stg_game_stat_ids_unique

    # Test there are no duplicates
    assert len(stg_game_stat_ids) == len(stg_game_stat_ids_unique)

    # Test that the loaded entities are valid
    model_checker(GameTeamStats, stg_game_stats)


def test_team_dataset(db_client: DbConnection, model_checker, subtests):
    ds = TeamDataset(years=[2025], classifications=["fbs"])

    # Test the transformation and loading into staging
    ops = []
    result = ds.transform(db_client, ops)
    assert result == True

    result: ClientBulkWriteResult = db_client.bulk_write(ops)
    assert result.inserted_count == len(ops)

    ext_teams = list(
        db_client.get_cfb_collection(
            Databases.extraction, ExtractionCollections.team
        ).find({})
    )
    stg_teams = list(db_client.get_cfb_collection(Databases.staging, Team).find({}))
    stg_team_exts = list(
        db_client.get_cfb_collection(Databases.staging, TeamExt).find({})
    )
    stg_venues = list(db_client.get_cfb_collection(Databases.staging, Venue).find({}))
    stg_conferences = list(
        db_client.get_cfb_collection(Databases.staging, Conference).find({})
    )

    stg_team_ids = [team["team_id"] for team in stg_teams]
    stg_team_ext_ids = [team_ext["team_id"] for team_ext in stg_team_exts]
    stg_venue_ids = [venue["venue_id"] for venue in stg_venues]
    stg_conference_ids = [conference["conference_id"] for conference in stg_conferences]
    stg_team_ids_unique = set(stg_team_ids)
    stg_team_ext_ids_unique = set(stg_team_ext_ids)
    stg_venue_ids_unique = set(stg_venue_ids)
    stg_conference_ids_unique = set(stg_conference_ids)

    ext_team_ids = {team["id"] for team in ext_teams}
    stg_team_venues = {team["venue_id"] for team in stg_teams}
    stg_team_conferences = {team["conference_id"] for team in stg_teams}

    # Test the appropriate entities are being loaded
    assert len(stg_team_ids) > 0
    assert len(stg_team_ext_ids) > 0
    assert len(stg_venue_ids) > 0
    assert len(stg_conference_ids) > 0

    assert ext_team_ids == stg_team_ids_unique
    assert stg_team_ids_unique == stg_team_ext_ids_unique
    assert stg_team_venues == stg_venue_ids_unique
    assert stg_team_conferences == stg_conference_ids_unique

    # Test there are no duplicates
    assert len(stg_team_ids) == len(stg_team_ids_unique)
    assert len(stg_team_ext_ids) == len(stg_team_ext_ids_unique)
    assert len(stg_venue_ids) == len(stg_venue_ids_unique)
    assert len(stg_conference_ids) == len(stg_conference_ids_unique)

    # Test that the loaded entities are valid
    model_checker(Team, stg_teams)
    model_checker(TeamExt, stg_team_exts)
    model_checker(Venue, stg_venues)
    model_checker(Conference, stg_conferences)
