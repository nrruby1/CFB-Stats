from datetime import datetime, time
from enum import Enum
from typing import Optional, Type, override
from pydantic import (
    ConfigDict,
    Field,
    StrictInt,
    StrictStr,
    StrictBool,
    StrictFloat,
    conlist,
)
from pydantic_mongo import AbstractRepository, PydanticObjectId
from .cfb_model import CfbBaseModel


class SeasonType(Enum):
    REGULAR = "regular"
    POSTSEASON = "postseason"


class Game(CfbBaseModel):

    game_id: StrictInt = Field(...)
    season: StrictInt = Field(...)
    week: StrictInt = Field(...)
    season_type: SeasonType = Field(...)
    start_date: datetime = Field(...)
    start_time_tbd: StrictBool = Field(...)
    completed: StrictBool = Field(...)
    neutral_site: StrictBool = Field(...)
    conference_game: StrictBool = Field(...)
    attendance: Optional[StrictInt] = Field(...)
    venue_id: Optional[StrictInt] = Field(...)
    home_id: StrictInt = Field(...)
    away_id: StrictInt = Field(...)
    winning_team_id: Optional[StrictInt] = Field(...)
    notes: Optional[StrictStr] = Field(...)

    @override
    def get_model_query(self) -> dict:
        return {"game_id": self.game_id}

    @override
    @staticmethod
    def model_id() -> str:
        return "game"

    @override
    @staticmethod
    def model_repository() -> Type[AbstractRepository]:
        from . import game_repository

        return game_repository.GameRepository

    @override
    def __eq__(self, value):
        if not isinstance(value, Game):
            return False

        return self.game_id == value.game_id


class GameTeamStats(CfbBaseModel):

    game_id: StrictInt = Field(...)
    team_id: StrictInt = Field(...)
    points: StrictInt = Field(...)
    line_scores: conlist(StrictInt) = Field(...)  # type: ignore
    possession_time: Optional[time] = Field(...)
    total_yards: Optional[StrictInt] = Field(...)

    rushing_yards: Optional[StrictInt] = Field(...)
    rushing_attempts: Optional[StrictInt] = Field(...)
    yards_per_rush_attempt: Optional[StrictFloat] = Field(...)
    rushing_tds: Optional[StrictInt] = Field(...)

    passing_yards: Optional[StrictInt] = Field(...)
    completions: Optional[StrictInt] = Field(...)
    passing_attempts: Optional[StrictInt] = Field(...)
    yards_per_pass: Optional[StrictFloat] = Field(...)
    yards_per_completion: Optional[StrictFloat] = Field(...)
    passing_tds: Optional[StrictInt] = Field(...)

    total_penalties: Optional[StrictInt] = Field(...)
    total_penalties_yards: Optional[StrictInt] = Field(...)

    first_downs: Optional[StrictInt] = Field(...)
    third_down_eff: Optional[conlist(StrictInt)] = Field(...)  # type: ignore
    fourth_down_eff: Optional[conlist(StrictInt)] = Field(...)  # type: ignore

    turnovers: Optional[StrictInt] = Field(...)
    total_fumbles: Optional[StrictInt] = Field(...)
    fumbles_lost: Optional[StrictInt] = Field(...)
    interceptions: Optional[StrictInt] = Field(...)

    tackles: Optional[StrictInt] = Field(...)
    tackles_for_loss: Optional[StrictInt] = Field(...)
    qb_hurries: Optional[StrictInt] = Field(...)
    sacks: Optional[StrictInt] = Field(...)
    passes_deflected: Optional[StrictInt] = Field(...)

    fumbles_recovered: Optional[StrictInt] = Field(...)
    passes_intercepted: Optional[StrictInt] = Field(...)
    interception_tds: Optional[StrictInt] = Field(...)
    interception_yards: Optional[StrictInt] = Field(...)
    defensive_tds: Optional[StrictInt] = Field(...)

    kicking_points: Optional[StrictInt] = Field(...)
    kick_returns: Optional[StrictInt] = Field(...)
    kick_return_tds: Optional[StrictInt] = Field(...)
    kick_return_yards: Optional[StrictInt] = Field(...)

    punt_returns: Optional[StrictInt] = Field(...)
    punt_return_tds: Optional[StrictInt] = Field(...)
    punt_return_yards: Optional[StrictInt] = Field(...)

    @override
    def get_model_query(self) -> dict:
        return {"game_id": self.game_id, "team_id": self.team_id}

    @override
    @staticmethod
    def model_id() -> str:
        return "game_team_stats"

    @override
    @staticmethod
    def model_repository() -> Type[AbstractRepository]:
        from . import game_repository

        return game_repository.GameTeamStatsRepository

    @override
    def __eq__(self, value):
        if not isinstance(value, GameTeamStats):
            return False

        return self.game_id == value.game_id and self.team_id == value.team_id
