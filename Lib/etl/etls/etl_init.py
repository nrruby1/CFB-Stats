import logging

from db.model.game_repository import *
from etl.etls.etl import *
from etl.datasets.team_dataset import TeamDataset
from etl.datasets.game_dataset import GameDataset
from etl.datasets.game_stats_dataset import GameStatsDataset

log = logging.getLogger("CfbStats.etl")


class EtlInit(EtlBase):
    """
    Transfers all CFB data from previous seasons.
    """

    def __init__(
        self,
        *,
        clean_extract: bool = True,
        clean_staging: bool = True,
        years: list[str] = [2023, 2024, 2025],
        classifications: list[str] = ["fbs", "fcs"],
        test_mode: bool = False,
    ):

        super().__init__(
            clean_extract=clean_extract,
            clean_staging=clean_staging,
            test_mode=test_mode,
        )

        self.datasets = [
            TeamDataset(years, classifications),
            GameDataset(years, classifications),
            GameStatsDataset(years, classifications),
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
