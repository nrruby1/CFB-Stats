import logging
from db.model.game import SeasonType
from db.model.game_repository import *
from db.db_connection import *
from etl.datasets.game_dataset import GameDataset
from etl.datasets.game_stats_dataset import GameStatsDataset
from etl.etls.etl import EtlBase

log = logging.getLogger("CfbStats.etl.etls")


class EtlWeeklyResults(EtlBase):
    """
    Transfers game and game team statistics data for a single week.
    """

    def __init__(
        self,
        *,
        year: int,
        week: int,
        season_type: SeasonType,
        classifications: list[str] = ["fbs", "fcs"],
        skip_extract: bool = False,
        clean_extract: bool = True,
        clean_staging: bool = True,
        test_mode: bool = False,
    ):

        super().__init__(
            name="Weekly Results",
            skip_extract=skip_extract,
            clean_extract=clean_extract,
            clean_staging=clean_staging,
            test_mode=test_mode,
        )

        self.year = year
        self.week = week
        self.season_type = season_type
        self.classifications = classifications

        self.datasets = [
            GameDataset(
                years=[year],
                classifications=classifications,
                weeks=[week],
                season_types=[season_type],
            ),
            GameStatsDataset(
                years=[year],
                classifications=classifications,
                weeks=[week],
                season_types=[season_type],
            ),
        ]

    def post_transform(self, db_client: DbConnection) -> bool:
        return True

    def validate(self, db_client: DbConnection) -> bool:
        stage_game_repo: GameRepository = db_client.get_cfb_repository(
            Databases.staging, Game
        )
        stage_game_team_stats_repo: GameTeamStatsRepository = (
            db_client.get_cfb_repository(Databases.staging, GameTeamStats)
        )

        games = list(stage_game_repo.find_by({}))
        game_team_stats = stage_game_team_stats_repo.find_by({})
        for game_team_stat in game_team_stats:
            game = next(
                (game for game in games if game.game_id == game_team_stat.game_id), None
            )
            if game is None:
                log.error(
                    f"Validation failed: No game found for game team stat with game id {game_team_stat.game_id}"
                )
                return False

        return True
