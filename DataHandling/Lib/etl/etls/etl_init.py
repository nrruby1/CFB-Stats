import logging

from db.model.game import SeasonType
from db.model.game_repository import *
from etl.etls.etl import *
from etl.datasets.team_dataset import TeamDataset
from etl.datasets.game_dataset import GameDataset
from etl.datasets.game_stats_dataset import GameStatsDataset

log = logging.getLogger("CfbStats.etl.etls")


class EtlInit(EtlBase):
    """
    Transfers all CFB data from previous seasons.
    """

    def __init__(
        self,
        *,
        skip_extract: bool = False,
        clean_extract: bool = True,
        clean_staging: bool = True,
        years: list[str] = [2023, 2024, 2025],
        classifications: list[str] = ["fbs", "fcs"],
        test_mode: bool = False,
    ):

        super().__init__(
            name="Initial",
            skip_extract=skip_extract,
            clean_extract=clean_extract,
            clean_staging=clean_staging,
            test_mode=test_mode,
        )

        weeks = list(range(1, 17))
        season_types = [
            SeasonType.REGULAR,
            SeasonType.POSTSEASON,
        ]

        self.datasets = [
            TeamDataset(years=years, classifications=classifications),
            GameDataset(
                years=years,
                weeks=weeks,
                classifications=classifications,
                season_types=season_types,
            ),
            GameStatsDataset(
                years=years,
                weeks=weeks,
                classifications=classifications,
                season_types=season_types,
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
