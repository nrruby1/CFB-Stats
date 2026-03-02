from datetime import datetime, time
from typing import Optional, Type, Union
from pydantic import (
    ConfigDict,
    Field,
    StrictInt,
    StrictStr,
    StrictBool,
    StrictFloat,
    conlist,
)
from pydantic_mongo import AbstractRepository
from .cfb_model import CfbBaseModel


class Game(CfbBaseModel):

    model_config = ConfigDict(
        validate_by_name=True, validate_assignment=True, arbitrary_types_allowed=True
    )

    game_id: StrictInt = Field(..., alias="gameId")
    season: StrictInt = Field(...)
    week: StrictInt = Field(...)
    season_type: StrictStr = Field(default=..., alias="seasonType")
    start_date: datetime = Field(default=..., alias="startDate")
    start_time_tbd: StrictBool = Field(default=..., alias="startTimeTBD")
    completed: StrictBool = Field(...)
    neutral_site: StrictBool = Field(default=..., alias="neutralSite")
    conference_game: StrictBool = Field(default=..., alias="conferenceGame")
    attendance: Optional[StrictInt] = Field(...)
    venue_id: Optional[StrictInt] = Field(default=..., alias="venueId")
    home_id: StrictInt = Field(default=..., alias="homeId")
    away_id: StrictInt = Field(default=..., alias="awayId")
    winning_team_id: Optional[StrictInt] = Field(default=..., alias="winningTeamId")
    notes: Optional[StrictStr] = Field(...)

    @staticmethod
    def model_id() -> str:
        return "game"

    @staticmethod
    def model_repository() -> Type[AbstractRepository]:
        from . import game_repository

        return game_repository.GameRepository


class GameTeamStats(CfbBaseModel):

    model_config = ConfigDict(
        validate_by_name=True, validate_assignment=True, arbitrary_types_allowed=True
    )

    game_id: StrictInt = Field(..., alias="gameId")
    team_id: StrictInt = Field(..., alias="teamId")
    points: StrictInt = Field(default=..., alias="points")
    line_scores: conlist(StrictInt) = Field(default=..., alias="lineScores")  # type: ignore
    possession_time: Optional[time] = Field(default=..., alias="possessionTime")
    total_yards: Optional[StrictInt] = Field(default=..., alias="totalYards")

    rushing_yards: Optional[StrictInt] = Field(default=..., alias="rushingYards")
    rushing_attempts: Optional[StrictInt] = Field(default=..., alias="rushingAttempts")
    yards_per_rush_attempt: Optional[StrictFloat] = Field(
        default=..., alias="yardsPerRushAttempt"
    )
    rushing_tds: Optional[StrictInt] = Field(default=..., alias="rushingTDs")

    passing_yards: Optional[StrictInt] = Field(default=..., alias="passingYards")
    completions: Optional[StrictInt] = Field(default=..., alias="completions")
    passing_attempts: Optional[StrictInt] = Field(default=..., alias="passingAttempts")
    yards_per_pass: Optional[StrictFloat] = Field(default=..., alias="yardsPerPass")
    yards_per_completion: Optional[StrictFloat] = Field(
        default=..., alias="yardsPerCompletion"
    )
    passing_tds: Optional[StrictInt] = Field(default=..., alias="passingTDs")

    total_penalties: Optional[StrictInt] = Field(default=..., alias="totalPenalties")
    total_penalties_yards: Optional[StrictInt] = Field(
        default=..., alias="totalPenaltiesYards"
    )

    first_downs: Optional[StrictInt] = Field(default=..., alias="firstDowns")
    third_down_eff: Optional[conlist(StrictInt)] = Field(default=..., alias="thirdDownEff")  # type: ignore
    fourth_down_eff: Optional[conlist(StrictInt)] = Field(default=..., alias="fourthDownEff")  # type: ignore

    turnovers: Optional[StrictInt] = Field(default=..., alias="turnovers")
    total_fumbles: Optional[StrictInt] = Field(default=..., alias="totalFumbles")
    fumbles_lost: Optional[StrictInt] = Field(default=..., alias="fumblesLost")
    interceptions: Optional[StrictInt] = Field(default=..., alias="interceptions")

    tackles: Optional[StrictInt] = Field(default=..., alias="tackles")
    tackles_for_loss: Optional[StrictInt] = Field(default=..., alias="tacklesForLoss")
    qb_hurries: Optional[StrictInt] = Field(default=..., alias="qbHurries")
    sacks: Optional[StrictInt] = Field(default=..., alias="sacks")
    passes_deflected: Optional[StrictInt] = Field(default=..., alias="passesDeflected")

    fumbles_recovered: Optional[StrictInt] = Field(
        default=..., alias="fumblesRecovered"
    )
    passes_intercepted: Optional[StrictInt] = Field(
        default=..., alias="passesIntercepted"
    )
    interception_tds: Optional[StrictInt] = Field(default=..., alias="interceptionTDs")
    interception_yards: Optional[StrictInt] = Field(
        default=..., alias="interceptionYards"
    )
    defensive_tds: Optional[StrictInt] = Field(default=..., alias="defensiveTDs")

    kicking_points: Optional[StrictInt] = Field(default=..., alias="kickingPoints")
    kick_returns: Optional[StrictInt] = Field(default=..., alias="kickReturns")
    kick_return_tds: Optional[StrictInt] = Field(default=..., alias="kickReturnTDs")
    kick_return_yards: Optional[StrictInt] = Field(default=..., alias="kickReturnYards")

    punt_returns: Optional[StrictInt] = Field(default=..., alias="puntReturns")
    punt_return_tds: Optional[StrictInt] = Field(default=..., alias="puntReturnTDs")
    punt_return_yards: Optional[StrictInt] = Field(default=..., alias="puntReturnYards")

    @staticmethod
    def model_id() -> str:
        return "game_team_stats"

    @staticmethod
    def model_repository() -> Type[AbstractRepository]:
        from . import game_repository

        return game_repository.GameTeamStatsRepository
