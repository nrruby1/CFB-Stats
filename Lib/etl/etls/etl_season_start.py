from aenum import skip
from db.model.game import SeasonType
from etl.etls.etl import *
from etl.datasets.game_dataset import GameDataset
from etl.datasets.team_dataset import TeamDataset

log = logging.getLogger("CfbStats.etl.etls")


class EtlSeasonStart(EtlBase):
    """
    Transforms team and game data for the start of a season.
    """

    def __init__(
        self,
        *,
        skip_extract: bool = False,
        clean_extract: bool = True,
        clean_staging: bool = True,
        years: list[str] = [2026],
        classifications: list[str] = ["fbs", "fcs"],
        test_mode: bool = False,
    ):

        super().__init__(
            name="Season Start",
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
            TeamDataset(years, classifications),
            GameDataset(
                years=years,
                weeks=weeks,
                classifications=classifications,
                season_types=season_types,
            ),
        ]

    def post_transform(self, db_client: DbConnection) -> bool:
        return True

    def validate(self, db_client: DbConnection) -> bool:
        return True
