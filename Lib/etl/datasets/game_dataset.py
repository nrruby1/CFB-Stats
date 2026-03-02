import logging
from db.db_connection import DbConnection, Databases, ExtractionCollections
from db.db_cleanup import cleanup_staging_collections
from db.model.game import Game
from db.model.game_repository import GameRepository
from etl.etls.etl import *
from etl.datasets.dataset_utility import *
from etl.datasets.extraction_datasets import *

log = logging.getLogger("CfbStats.etl")


class GameDataset(DataSet):
    """
    Transfers game data from a given list of years and conference classifications.
    """

    def __init__(
        self, years: list[int], classifications: list[str], weeks: list[int] = None
    ):
        super().__init__()
        self.years = years
        self.classifications = classifications
        self.weeks = weeks

        self.extract_datasets = {
            ExtractGamesDataSet(
                year_list=years, class_list=classifications, week_list=weeks
            ),
            ExtractVenueDataSet(),
        }

    def transform(self, db_client: DbConnection) -> bool:
        """
        Transform game data from extraction database to staging database.
        """
        try:
            extr_game_coll = db_client.get_cfb_collection(
                Databases.extraction, ExtractionCollections.game
            )
            stage_game_repo: GameRepository = db_client.get_cfb_repository(
                Databases.staging, Game
            )

            count = 0
            extr_games = extr_game_coll.find()
            for extr_game in extr_games:
                validate_fields = validate_mandatory_fields(
                    extr_game,
                    "id",
                    "season",
                    "week",
                    "seasonType",
                    "completed",
                    "startTimeTBD",
                    "homeId",
                    "awayId",
                )
                if not validate_fields:
                    log.warning(
                        f"GameDataset: Skipping game with id {extr_game.get('id')} due to missing mandatory field(s)"
                    )
                    continue

                winning_team_id = None
                if extr_game.get("completed") is True:
                    validate_fields = validate_mandatory_fields(
                        extr_game, "homePoints", "awayPoints"
                    )
                    if not validate_fields:
                        log.warning(
                            f"GameDataset: Skipping completed game with id {extr_game.get('id')} due to missing mandatory field(s)"
                        )
                        continue

                    if extr_game.get("homePoints") > extr_game.get("awayPoints"):
                        winning_team_id = extr_game.get("homeId")
                    else:
                        winning_team_id = extr_game.get("awayId")

                venue = get_or_create_venue(
                    db_client, extr_game.get("venueId"), count=count
                )
                if venue is None:
                    log.warning(f"GameDataset: {extr_game.get('id')} has no venue")

                game = Game(
                    gameId=extr_game.get("id"),
                    season=extr_game.get("season"),
                    week=extr_game.get("week"),
                    seasonType=extr_game.get("seasonType"),
                    startDate=extr_game.get("startDate"),
                    startTimeTBD=extr_game.get("startTimeTBD"),
                    completed=extr_game.get("completed"),
                    neutralSite=extr_game.get("neutralSite"),
                    conferenceGame=extr_game.get("conferenceGame"),
                    attendance=extr_game.get("attendance"),
                    venueId=venue.venue_id if venue is not None else None,
                    homeId=extr_game.get("homeId"),
                    awayId=extr_game.get("awayId"),
                    winningTeamId=winning_team_id,
                    notes=extr_game.get("notes"),
                )

                Game.model_validate(game)
                stage_game_repo.save(game)
                count += 1

            log.debug(f"GameDataset: Transformed {count} entities")
            return True
        except Exception as e:
            log.exception(f"GameDataset: Exception during transform: {e}")
            return False

    def load(self, db_client: DbConnection):
        """
        Load game data from staging database to production database.
        """
        try:
            stage_game_repo, prod_game_repo = get_repos(db_client, Game)

            query = lambda game: {"gameId": game.game_id}
            load_into_production(
                prod_repo=prod_game_repo,
                stage_repo=stage_game_repo,
                query=query,
                replace=True,
            )
        except Exception as e:
            log.exception(f"GameDataset: Exception during load: {e}")

    def cleanup(self, db_client: DbConnection):
        """
        Clean up game data from staging database.
        """
        try:
            cleanup_staging_collections(db_client, Game)
        except Exception as e:
            log.exception(f"GameDataset: Exception during cleanup: {e}")
