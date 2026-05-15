import logging
from datetime import time
from typing import Optional
from db.db_connection import *
from db.db_cleanup import cleanup_staging_collections
from db.model.game import GameTeamStats, SeasonType
from db.model.game_repository import GameTeamStatsRepository
from etl.etls.etl import *
from etl.datasets.dataset_utility import *
from etl.datasets.extraction_datasets import ExtractGameTeamStats, ExtractGamesDataSet

log = logging.getLogger("CfbStats.etl.datasets")


class GameStatsDataset(DataSet):
    """
    Transfers game statistics data from a given list of years and conference classifications.
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
            ExtractGameTeamStats(
                year_list=years, week_list=weeks, season_types=self.season_types
            ),
        }

        self.models = {GameTeamStats: True}

    def transform(self, db_client: DbConnection, operations: list) -> bool:
        """
        Transform game statistics data from extraction database to staging database.
        """
        try:
            extr_games_coll = db_client.get_cfb_collection(
                Databases.extraction, ExtractionCollections.game
            )

            extr_game_stats_coll = db_client.get_cfb_collection(
                Databases.extraction, ExtractionCollections.game_team_stats
            )

            count = 0
            extr_game_stats = extr_game_stats_coll.find()
            for extr_game_stat in extr_game_stats:
                validate_fields = validate_mandatory_fields(
                    extr_game_stat, "id", "teams"
                )
                if not validate_fields:
                    log.warning(
                        f"GameStatsDataset: Skipping game stat with id {extr_game_stat.get('id')} due to missing mandatory field(s)"
                    )
                    continue

                extr_game = extr_games_coll.find_one({"id": extr_game_stat.get("id")})
                if extr_game is None:
                    continue

                home_team_stat = self.create_game_team_stat(
                    extr_game_stat,
                    extr_game.get("homeId"),
                    extr_game.get("homeLineScores", []),
                )
                if home_team_stat is None:
                    log.warning(
                        f"GameStatsDataset: Skipping game stat with id {extr_game_stat.get('id')} due to missing home team stat"
                    )
                    continue

                away_team_stat = self.create_game_team_stat(
                    extr_game_stat,
                    extr_game.get("awayId"),
                    extr_game.get("awayLineScores", []),
                )
                if away_team_stat is None:
                    log.warning(
                        f"GameStatsDataset: Skipping game stat with id {extr_game_stat.get('id')} due to missing away team stat"
                    )
                    continue

                ops = insert_many_operations(
                    db_client=db_client,
                    db=Databases.staging,
                    entities=(home_team_stat, away_team_stat),
                    do_replace=False,
                )
                if len(ops) == 2:
                    operations.extend(ops)
                    count += 2
                else:
                    log.warning(
                        f"GameStatsDataset: Failed to create insert operations for game stat with id {extr_game_stat.get('id')}"
                    )

            log.debug(f"GameStatsDataset: Transformed {count} entities")
            return True
        except Exception as e:
            log.exception(f"GameStatsDataset: Exception during transform: {e}")
            return False

    def create_game_team_stat(
        self, extr_game_stat: dict, team_id: int, line_scores: list[int]
    ) -> GameTeamStats | None:

        extr_team_stat = next(
            (
                team_stat
                for team_stat in extr_game_stat.get("teams", [])
                if team_stat.get("teamId") == team_id
            ),
            None,
        )
        if extr_team_stat is None:
            return None

        completion_attempts = extr_team_stat.get("completionAttempts")
        completions = (
            completion_attempts[0] if completion_attempts is not None else None
        )
        passing_attempts = (
            completion_attempts[1] if completion_attempts is not None else None
        )
        passing_yards = extr_team_stat.get("netPassingYards")
        rushing_attempts = extr_team_stat.get("rushingAttempts")
        rushing_yards = extr_team_stat.get("rushingYards")

        third_down_eff = self.parse_efficiency_stat(extr_team_stat.get("thirdDownEff"))
        fourth_down_eff = self.parse_efficiency_stat(
            extr_team_stat.get("fourthDownEff")
        )

        penalties_data = self.parse_penalty_stat(
            extr_team_stat.get("totalPenaltiesYards")
        )
        total_penalties = penalties_data[0] if penalties_data else None
        total_penalties_yards = penalties_data[1] if penalties_data else None

        possession_time = self.parse_possession_time(
            extr_team_stat.get("possessionTime")
        )

        yards_per_completion = self.calculate_yards_per_attempt(
            passing_yards, completions
        )
        yards_per_pass = self.calculate_yards_per_attempt(
            passing_yards, passing_attempts
        )
        yards_per_rush_attempt = self.calculate_yards_per_attempt(
            rushing_yards, rushing_attempts
        )

        game_stat = GameTeamStats(
            game_id=extr_game_stat.get("id"),
            team_id=team_id,
            points=extr_team_stat.get("points"),
            line_scores=line_scores,
            possession_time=possession_time,
            total_yards=extr_team_stat.get("totalYards"),
            rushing_yards=rushing_yards,
            rushing_attempts=rushing_attempts,
            yards_per_rush_attempt=yards_per_rush_attempt,
            rushing_tds=extr_team_stat.get("rushingTDs"),
            passing_yards=passing_yards,
            completions=completions,
            passing_attempts=passing_attempts,
            yards_per_pass=yards_per_pass,
            yards_per_completion=yards_per_completion,
            passing_tds=extr_team_stat.get("passingTDs"),
            total_penalties=total_penalties,
            total_penalties_yards=total_penalties_yards,
            first_downs=extr_team_stat.get("firstDowns"),
            third_down_eff=third_down_eff,
            fourth_down_eff=fourth_down_eff,
            turnovers=extr_team_stat.get("turnovers"),
            total_fumbles=extr_team_stat.get("totalFumbles"),
            fumbles_lost=extr_team_stat.get("fumblesLost"),
            interceptions=extr_team_stat.get("interceptions"),
            tackles=extr_team_stat.get("tackles"),
            tackles_for_loss=extr_team_stat.get("tacklesForLoss"),
            qb_hurries=extr_team_stat.get("qbHurries"),
            sacks=extr_team_stat.get("sacks"),
            passes_deflected=extr_team_stat.get("passesDeflected"),
            fumbles_recovered=extr_team_stat.get("fumblesRecovered"),
            passes_intercepted=extr_team_stat.get("passesIntercepted"),
            interception_tds=extr_team_stat.get("interceptionTDs"),
            interception_yards=extr_team_stat.get("interceptionYards"),
            defensive_tds=extr_team_stat.get("defensiveTDs"),
            kicking_points=extr_team_stat.get("kickingPoints"),
            kick_returns=extr_team_stat.get("kickReturns"),
            kick_return_tds=extr_team_stat.get("kickReturnTDs"),
            kick_return_yards=extr_team_stat.get("kickReturnYards"),
            punt_returns=extr_team_stat.get("puntReturns"),
            punt_return_tds=extr_team_stat.get("puntReturnTDs"),
            punt_return_yards=extr_team_stat.get("puntReturnYards"),
        )

        return game_stat

    def parse_efficiency_stat(self, stat_str: Optional[str]) -> Optional[list[int]]:
        """Parse efficiency stat from 'X-Y' format to [X, Y] list."""
        if stat_str is None:
            return None
        try:
            parts = stat_str.split("-")
            return [int(parts[0]), int(parts[1])]
        except (ValueError, IndexError):
            return None

    def parse_penalty_stat(self, stat_str: Optional[str]) -> Optional[tuple[int, int]]:
        """Parse penalty stat from 'X-Y' format to (X, Y) tuple."""
        if stat_str is None:
            return None
        try:
            parts = stat_str.split("-")
            return (int(parts[0]), int(parts[1]))
        except (ValueError, IndexError):
            return None

    def parse_possession_time(self, time_str: Optional[str]) -> Optional[time]:
        """Parse possession time from 'MM:SS' format to time object."""
        if time_str is None:
            return None
        try:
            parts = time_str.split(":")
            minutes = int(parts[0])
            seconds = int(parts[1])
            return time(minute=minutes, second=seconds)
        except (ValueError, IndexError):
            return None

    def calculate_yards_per_attempt(
        self, yards: Optional[int], attempts: Optional[int]
    ) -> Optional[float]:

        if yards is None or attempts is None:
            return None
        elif attempts <= 0:
            return 0
        else:
            return round(yards / attempts, 1)
