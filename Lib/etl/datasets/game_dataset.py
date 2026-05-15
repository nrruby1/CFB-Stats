import logging
from db.db_connection import DbConnection, Databases, ExtractionCollections
from db.db_cleanup import cleanup_staging_collections
from db.model.game import Game
from db.model.game_repository import GameRepository
from etl.etls.etl import *
from etl.datasets.dataset_utility import *
from etl.datasets.extraction_datasets import *

log = logging.getLogger("CfbStats.etl.datasets")


class GameDataset(DataSet):
    """
    Transfers game data from a given list of years and conference classifications.
    """

    def __init__(
        self,
        years: list[int],
        classifications: list[str],
        weeks: list[int],
        season_types: list[SeasonType],
    ):
        super().__init__()
        self.years = years
        self.classifications = classifications
        self.weeks = weeks
        self.season_types = season_types

        self.extract_datasets = {
            ExtractGamesDataSet(
                year_list=years,
                class_list=classifications,
                week_list=weeks,
                season_types=season_types,
            ),
            ExtractVenueDataSet(),
        }

        self.models = {Game: True, Venue: False}

    def transform(self, db_client: DbConnection, operations: list) -> bool:
        """
        Transform game data from extraction database to staging database.
        """
        try:
            extr_game_coll = db_client.get_cfb_collection(
                Databases.extraction, ExtractionCollections.game
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
                    db_client, extr_game.get("venueId"), operations, count=count
                )
                if venue is None:
                    log.warning(f"GameDataset: {extr_game.get('id')} has no venue")

                game = Game(
                    game_id=extr_game.get("id"),
                    season=extr_game.get("season"),
                    week=extr_game.get("week"),
                    season_type=extr_game.get("seasonType"),
                    start_date=extr_game.get("startDate"),
                    start_time_tbd=extr_game.get("startTimeTBD"),
                    completed=extr_game.get("completed"),
                    neutral_site=extr_game.get("neutralSite"),
                    conference_game=extr_game.get("conferenceGame"),
                    attendance=extr_game.get("attendance"),
                    venue_id=venue.venue_id if venue is not None else None,
                    home_id=extr_game.get("homeId"),
                    away_id=extr_game.get("awayId"),
                    winning_team_id=winning_team_id,
                    notes=extr_game.get("notes"),
                )

                Game.model_validate(game)
                op = insert_one_operation(
                    db_client=db_client,
                    db=Databases.staging,
                    entity=game,
                    do_replace=False,
                )
                if op is not None:
                    operations.append(op)
                    count += 1
                else:
                    log.warning(
                        f"GameDataset: Failed to create insert operation for game id {game.game_id}"
                    )

            log.debug(f"GameDataset: Transformed {count} entities")
            return True
        except Exception as e:
            log.exception(f"GameDataset: Exception during transform: {e}")
            return False
