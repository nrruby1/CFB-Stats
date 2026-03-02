import logging
from db.model.game_repository import *
from db.db_connection import *
from etl.datasets.game_dataset import GameDataset
from etl.datasets.game_stats_dataset import GameStatsDataset
from etl.etls.etl import EtlBase

log = logging.getLogger("CfbStats.etl")


class EtlWeeklyResults(EtlBase):
    """
    Transfers game and game team statistics data for a single week.
    """

    def __init__(
        self,
        *,
        year: int,
        week: int,
        classifications: list[str] = ["fbs", "fcs"],
        clean_extract: bool = True,
        clean_staging: bool = True,
        test_mode: bool = False,
    ):
        super().__init__(
            clean_extract=clean_extract,
            clean_staging=clean_staging,
            test_mode=test_mode,
        )

        self.year = year
        self.week = week
        self.classifications = classifications

        self.datasets = [
            GameDataset(
                years=[year],
                classifications=classifications,
                weeks=[week],
            ),
            GameStatsDataset(
                years=[year],
                classifications=classifications,
                weeks=[week],
            ),
        ]

    def post_transform(self) -> bool:
        return True

    def validate(self) -> bool:
        with DbConnection(self.test_mode) as db_client:
            stage_game_repo: GameRepository = db_client.get_cfb_repository(
                Databases.staging, Game
            )
            stage_game_team_stats_repo: GameTeamStatsRepository = (
                db_client.get_cfb_repository(Databases.staging, GameTeamStats)
            )

            game_team_stats = stage_game_team_stats_repo.find_by({})
            for game_team_stat in game_team_stats:
                game = stage_game_repo.find_game(game_team_stat.game_id)
                if game is None:
                    log.error(
                        f"Validation failed: No game found for game team stat with id {game_team_stat.id} and game id {game_team_stat.game_id}"
                    )
                    return False
        return True
