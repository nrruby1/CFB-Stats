import pytest
import logging

from db.db_connection import *
from db.model.game import SeasonType

from etl.etls.etl import *
from etl.etls.etl_init import EtlInit
from etl.etls.etl_season_start import EtlSeasonStart
from etl.etls.etl_weekly_results import EtlWeeklyResults

log = logging.getLogger("CfbStats.Tests.test_etls")

pytestmark = pytest.mark.usefixtures(
    "cleanup_db", "setup_extract_data", "cleanup_staging", "cleanup_production"
)


class EtlTester(EtlBase):
    def __init__(self, etl: EtlBase):
        super().__init__(
            name=etl.name,
            skip_extract=True,
            test_mode=True,
        )

        self.etl = etl
        self.datasets = etl.datasets

    def run_etl(self, db_client: DbConnection):
        log.info(f"Running {self.name} ETL tool")
        etl_timer = Timer(self.name)

        self.etl.calculate_datasets()

        # Extraction DB -> Staging DB
        transform_success = Timer("Transformation").run(
            lambda: self.etl.transform(db_client)
        )
        assert transform_success

        # Additional transformations
        log.info("Running post transformation")
        post_transform_success = Timer("Post Transformation").run(
            lambda: self.etl.post_transform(db_client)
        )
        assert post_transform_success

        # Validate Staging DB
        log.info("Running validation")
        validated = Timer("Validation").run(lambda: self.etl.validate(db_client))
        assert validated

        # Staging DB to Presentation DB
        with db_client.start_session() as session:
            timer = Timer("Loading")
            session.with_transaction(lambda s: self.etl.load(s, db_client))
            timer.stop_and_log()

        log.debug(etl_timer.stop())
        log.info(f"Finished running {self.name} ETL tool")

    def post_transform(self, db_client: DbConnection) -> bool:
        return False

    def validate(self, db_client: DbConnection) -> bool:
        return False


def test_etl_init(db_client: DbConnection, model_checker):
    etl = EtlInit(
        skip_extract=True, years=[2025], classifications=["fbs"], test_mode=True
    )

    EtlTester(etl).run_etl(db_client)

    ext_teams = list(
        db_client.get_cfb_collection(
            Databases.extraction, ExtractionCollections.team
        ).find({})
    )
    ext_games = list(
        db_client.get_cfb_collection(
            Databases.extraction, ExtractionCollections.game
        ).find({})
    )
    ext_game_team_stats = list(
        db_client.get_cfb_collection(
            Databases.extraction, ExtractionCollections.game_team_stats
        ).find({})
    )

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

    ext_team_ids = set(team["id"] for team in ext_teams)
    expected_conference_ids = set(team["conference_id"] for team in prd_teams)
    ext_game_ids = set(game["id"] for game in ext_games)
    ext_game_stat_ids = {
        str(stat["id"]) + "_" + str(team["teamId"])
        for stat in ext_game_team_stats
        for team in stat["teams"]
    }
    expected_venue_ids = set(team["venue_id"] for team in prd_teams) | set(
        game["venue_id"] for game in prd_games
    )

    # Test the appropriate entities are being loaded
    assert len(prd_team_ids) > 0
    assert len(prd_team_ext_ids) > 0
    assert len(prd_game_ids) > 0
    assert len(prd_game_stat_ids) > 0
    assert len(prd_conference_ids) > 0
    assert len(prd_venue_ids) > 0

    assert set(prd_team_ids) == ext_team_ids
    assert set(prd_team_ext_ids) == ext_team_ids
    assert set(prd_conference_ids) == expected_conference_ids
    assert set(prd_game_ids) == ext_game_ids
    assert set(prd_game_stat_ids) == ext_game_stat_ids
    assert set(prd_venue_ids) == expected_venue_ids

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
        skip_extract=True, years=[2025], classifications=["fbs"], test_mode=True
    )

    EtlTester(etl).run_etl(db_client)

    ext_teams = list(
        db_client.get_cfb_collection(
            Databases.extraction, ExtractionCollections.team
        ).find({})
    )
    ext_games = list(
        db_client.get_cfb_collection(
            Databases.extraction, ExtractionCollections.game
        ).find({})
    )

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

    ext_team_ids = set(team["id"] for team in ext_teams)
    ext_game_ids = set(game["id"] for game in ext_games)
    expected_venue_ids = set(
        team["venue_id"] for team in prd_teams if team.get("venue_id") is not None
    ) | set(game["venue_id"] for game in prd_games if game.get("venue_id") is not None)
    expected_conference_ids = set(team["conference_id"] for team in prd_teams)

    # Test the appropriate entities are being loaded
    assert len(prd_team_ids) > 0
    assert len(prd_team_ext_ids) > 0
    assert len(prd_game_ids) > 0
    assert len(prd_conference_ids) > 0
    assert len(prd_venue_ids) > 0

    assert set(prd_team_ids) == ext_team_ids
    assert set(prd_team_ext_ids) == ext_team_ids
    assert set(prd_game_ids) == ext_game_ids
    assert set(prd_conference_ids) == expected_conference_ids
    assert set(prd_venue_ids) == expected_venue_ids

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
        skip_extract=True,
        test_mode=True,
    )

    EtlTester(etl).run_etl(db_client)

    ext_games = list(
        db_client.get_cfb_collection(
            Databases.extraction, ExtractionCollections.game
        ).find({})
    )
    ext_game_team_stats = list(
        db_client.get_cfb_collection(
            Databases.extraction, ExtractionCollections.game_team_stats
        ).find({})
    )

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

    ext_game_ids = {game["id"] for game in ext_games}
    ext_game_stat_ids = {
        str(stat["id"]) + "_" + str(team["teamId"])
        for stat in ext_game_team_stats
        for team in stat["teams"]
    }
    expected_venue_ids = set(game["venue_id"] for game in prd_games)

    # Test the appropriate entities are being loaded
    assert len(prd_game_ids) > 0
    assert len(prd_game_stat_ids) > 0
    assert len(prd_venue_ids) > 0

    assert prd_game_ids == ext_game_ids
    assert prd_game_stat_ids == ext_game_stat_ids
    assert prd_venue_ids == expected_venue_ids

    # Test there are no duplicates
    assert len(prd_games) == len(prd_game_ids)
    assert len(prd_game_stats) == len(prd_game_stat_ids)
    assert len(prd_venues) == len(prd_venue_ids)

    # Test that the loaded entities are valid
    model_checker(Game, prd_games)
    model_checker(GameTeamStats, prd_game_stats)
    model_checker(Venue, prd_venues)
