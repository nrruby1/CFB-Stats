import logging
from datetime import time
from typing import Optional
from db.db_connection import *
from db.db_cleanup import cleanup_staging_collections
from db.model.game import GameTeamStats
from db.model.game_repository import GameTeamStatsRepository
from etl.etls.etl import *
from etl.datasets.extraction_datasets import ExtractGameTeamStats, ExtractGamesDataSet

log = logging.getLogger("CfbStats.etl")


class GameStatsDataset(DataSet):
    """
    Transfers game statistics data from a given list of years and conference classifications.
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
            ExtractGameTeamStats(year_list=years, week_list=weeks),
        }

    def transform(self, db_client: DbConnection) -> bool:
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
            stage_game_stats_repo: GameTeamStatsRepository = (
                db_client.get_cfb_repository(Databases.staging, GameTeamStats)
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

                stage_game_stats_repo.save(home_team_stat)
                stage_game_stats_repo.save(away_team_stat)
                count += 2

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
            gameId=extr_game_stat.get("id"),
            teamId=team_id,
            points=extr_team_stat.get("points"),
            lineScores=line_scores,
            possessionTime=possession_time,
            totalYards=extr_team_stat.get("totalYards"),
            rushingYards=rushing_yards,
            rushingAttempts=rushing_attempts,
            yardsPerRushAttempt=yards_per_rush_attempt,
            rushingTDs=extr_team_stat.get("rushingTDs"),
            passingYards=passing_yards,
            completions=completions,
            passingAttempts=passing_attempts,
            yardsPerPass=yards_per_pass,
            yardsPerCompletion=yards_per_completion,
            passingTDs=extr_team_stat.get("passingTDs"),
            totalPenalties=total_penalties,
            totalPenaltiesYards=total_penalties_yards,
            firstDowns=extr_team_stat.get("firstDowns"),
            thirdDownEff=third_down_eff,
            fourthDownEff=fourth_down_eff,
            turnovers=extr_team_stat.get("turnovers"),
            totalFumbles=extr_team_stat.get("totalFumbles"),
            fumblesLost=extr_team_stat.get("fumblesLost"),
            interceptions=extr_team_stat.get("interceptions"),
            tackles=extr_team_stat.get("tackles"),
            tacklesForLoss=extr_team_stat.get("tacklesForLoss"),
            qbHurries=extr_team_stat.get("qbHurries"),
            sacks=extr_team_stat.get("sacks"),
            passesDeflected=extr_team_stat.get("passesDeflected"),
            fumblesRecovered=extr_team_stat.get("fumblesRecovered"),
            passesIntercepted=extr_team_stat.get("passesIntercepted"),
            interceptionTDs=extr_team_stat.get("interceptionTDs"),
            interceptionYards=extr_team_stat.get("interceptionYards"),
            defensiveTDs=extr_team_stat.get("defensiveTDs"),
            kickingPoints=extr_team_stat.get("kickingPoints"),
            kickReturns=extr_team_stat.get("kickReturns"),
            kickReturnTDs=extr_team_stat.get("kickReturnTDs"),
            kickReturnYards=extr_team_stat.get("kickReturnYards"),
            puntReturns=extr_team_stat.get("puntReturns"),
            puntReturnTDs=extr_team_stat.get("puntReturnTDs"),
            puntReturnYards=extr_team_stat.get("puntReturnYards"),
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

    def load(self, db_client: DbConnection):
        """
        Load game statistics data from staging database to production database.
        """
        try:
            stage_game_stats_repo, prod_game_stats_repo = get_repos(
                db_client, GameTeamStats
            )

            query = lambda stat: {"game_id": stat.game_id, "team_id": stat.team_id}
            load_into_production(
                stage_repo=stage_game_stats_repo,
                prod_repo=prod_game_stats_repo,
                query=query,
            )
        except Exception as e:
            log.exception(f"GameStatsDataset: Exception during load: {e}")

    def cleanup(self, db_client: DbConnection):
        """
        Clean up game statistics data from staging database.
        """
        try:
            cleanup_staging_collections(db_client, GameTeamStats)
        except Exception as e:
            log.exception(f"GameStatsDataset: Exception during cleanup: {e}")
