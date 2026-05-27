import pytest

from db.db_connection import *
from db.model.game import SeasonType

from etl.etls.etl import *
from etl.etls.etl_init import EtlInit
from etl.etls.etl_season_start import EtlSeasonStart
from etl.etls.etl_weekly_results import EtlWeeklyResults

log = logging.getLogger("CfbStats.Tests.test_etls_full")

pytestmark = pytest.mark.usefixtures(
    "ensure_isolation",
    "cleanup_db",
)


def test_etl_init(db_client: DbConnection, model_checker):
    etl = EtlInit(
        skip_extract=False,
        years=[2025],
        classifications=["fbs"],
        test_mode=True,
    )
    etl.run_etl()

    prd_teams = list(db_client.get_cfb_collection(Databases.production, Team).find({}))
    prd_team_exts = list(
        db_client.get_cfb_collection(Databases.production, TeamExt).find({})
    )
    prd_conferences = list(
        db_client.get_cfb_collection(Databases.production, Conference).find({})
    )
    prd_games = list(db_client.get_cfb_collection(Databases.production, Game).find({}))
    prd_game_stats = list(
        db_client.get_cfb_collection(Databases.production, GameTeamStats).find({})
    )
    prd_venues = list(
        db_client.get_cfb_collection(Databases.production, Venue).find({})
    )

    prd_team_ids = [team["team_id"] for team in prd_teams]
    prd_team_ext_ids = [team_ext["team_id"] for team_ext in prd_team_exts]
    prd_conference_ids = [conf["conference_id"] for conf in prd_conferences]
    prd_game_ids = [game["game_id"] for game in prd_games]
    prd_game_stat_ids = [
        str(stat["game_id"]) + "_" + str(stat["team_id"]) for stat in prd_game_stats
    ]
    prd_venue_ids = [venue["venue_id"] for venue in prd_venues]

    # Test entities are being loaded
    assert len(prd_team_ids) > 0
    assert len(prd_team_ext_ids) > 0
    assert len(prd_game_ids) > 0
    assert len(prd_game_stat_ids) > 0
    assert len(prd_conference_ids) > 0
    assert len(prd_venue_ids) > 0

    # Test there are no duplicates
    assert len(prd_team_ids) == len(set(prd_team_ids))
    assert len(prd_team_ext_ids) == len(set(prd_team_ext_ids))
    assert len(prd_conference_ids) == len(set(prd_conference_ids))
    assert len(prd_game_ids) == len(set(prd_game_ids))
    assert len(prd_game_stat_ids) == len(set(prd_game_stat_ids))
    assert len(prd_venue_ids) == len(set(prd_venue_ids))

    # Test that the loaded entities are valid
    model_checker(Team, prd_teams)
    model_checker(TeamExt, prd_team_exts)
    model_checker(Conference, prd_conferences)
    model_checker(Game, prd_games)
    model_checker(GameTeamStats, prd_game_stats)
    model_checker(Venue, prd_venues)


def test_etl_season_start(db_client: DbConnection, model_checker):
    etl = EtlSeasonStart(
        skip_extract=False,
        years=[2025],
        classifications=["fbs"],
        test_mode=True,
    )
    etl.run_etl()

    prd_teams = list(db_client.get_cfb_collection(Databases.production, Team).find({}))
    prd_team_exts = list(
        db_client.get_cfb_collection(Databases.production, TeamExt).find({})
    )
    prd_conferences = list(
        db_client.get_cfb_collection(Databases.production, Conference).find({})
    )
    prd_games = list(db_client.get_cfb_collection(Databases.production, Game).find({}))
    prd_venues = list(
        db_client.get_cfb_collection(Databases.production, Venue).find({})
    )

    prd_team_ids = [team["team_id"] for team in prd_teams]
    prd_team_ext_ids = [team_ext["team_id"] for team_ext in prd_team_exts]
    prd_conference_ids = [conf["conference_id"] for conf in prd_conferences]
    prd_game_ids = [game["game_id"] for game in prd_games]
    prd_venue_ids = [venue["venue_id"] for venue in prd_venues]

    # Test entities are being loaded
    assert len(prd_team_ids) > 0
    assert len(prd_team_ext_ids) > 0
    assert len(prd_game_ids) > 0
    assert len(prd_conference_ids) > 0
    assert len(prd_venue_ids) > 0

    # Test there are no duplicates
    assert len(prd_team_ids) == len(set(prd_team_ids))
    assert len(prd_team_ext_ids) == len(set(prd_team_ext_ids))
    assert len(prd_conference_ids) == len(set(prd_conference_ids))
    assert len(prd_game_ids) == len(set(prd_game_ids))
    assert len(prd_venue_ids) == len(set(prd_venue_ids))

    # Test that the loaded entities are valid
    model_checker(Team, prd_teams)
    model_checker(TeamExt, prd_team_exts)
    model_checker(Conference, prd_conferences)
    model_checker(Game, prd_games)
    model_checker(Venue, prd_venues)


def test_etl_weekly_results(db_client: DbConnection, model_checker):
    etl = EtlWeeklyResults(
        year=2025,
        week=1,
        season_type=SeasonType.REGULAR,
        classifications=["fbs"],
        skip_extract=False,
        test_mode=True,
    )
    etl.run_etl()

    prd_games = list(db_client.get_cfb_collection(Databases.production, Game).find({}))
    prd_game_stats = list(
        db_client.get_cfb_collection(Databases.production, GameTeamStats).find({})
    )
    prd_venues = list(
        db_client.get_cfb_collection(Databases.production, Venue).find({})
    )

    prd_game_ids = {game["game_id"] for game in prd_games}
    prd_game_stat_ids = {
        str(stat["game_id"]) + "_" + str(stat["team_id"]) for stat in prd_game_stats
    }
    prd_venue_ids = {venue["venue_id"] for venue in prd_venues}

    # Test the appropriate entities are being loaded
    assert len(prd_game_ids) > 0
    assert len(prd_game_stat_ids) > 0
    assert len(prd_venue_ids) > 0

    # Test there are no duplicates
    assert len(prd_games) == len(prd_game_ids)
    assert len(prd_game_stats) == len(prd_game_stat_ids)
    assert len(prd_venues) == len(prd_venue_ids)

    # Test that the loaded entities are valid
    model_checker(Game, prd_games)
    model_checker(GameTeamStats, prd_game_stats)
    model_checker(Venue, prd_venues)
