from etl.etls.etl import *
from etl.datasets.game_dataset import GameDataset
from etl.datasets.team_dataset import TeamDataset

log = logging.getLogger("CfbStats.etl")


class EtlSeasonStart(EtlBase):
    """
    Transforms team and game data for the start of a season.
    """

    def __init__(
        self,
        *,
        clean_extract: bool = True,
        clean_staging: bool = True,
        years: list[str] = [2026],
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
            GameDataset(years=years, classifications=classifications),
        ]

    def post_transform(self) -> bool:
        return True

    def validate(self) -> bool:
        return True
