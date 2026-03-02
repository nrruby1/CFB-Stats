from typing import Optional, Type
from pydantic import ConfigDict, Field, StrictInt, StrictStr, conlist
from pydantic_mongo import AbstractRepository
from .cfb_model import CfbBaseModel


class Team(CfbBaseModel):

    model_config = ConfigDict(validate_by_name=True, validate_assignment=True)

    team_id: StrictInt = Field(default=..., alias="teamId")
    year: StrictInt = Field(...)
    school: StrictStr = Field(...)
    conference_id: StrictInt = Field(default=..., alias="conferenceId")
    classification: StrictStr = Field(...)
    division: Optional[StrictStr] = Field(...)
    venue_id: Optional[StrictInt] = Field(default=..., alias="venueId")
    # __properties = ["id", "year", "school", "conferenceId", "division", "classification", "venueId"]

    @staticmethod
    def model_id() -> str:
        return "team"

    @staticmethod
    def model_repository() -> Type[AbstractRepository]:
        from . import team_repository

        return team_repository.TeamRepository


class TeamExt(CfbBaseModel):

    model_config = ConfigDict(validate_by_name=True, validate_assignment=True)

    team_id: StrictInt = Field(default=..., alias="teamId")
    year: StrictInt = Field(...)
    mascot: Optional[StrictStr] = Field(...)
    abbreviation: Optional[StrictStr] = Field(...)
    alternate_names: Optional[conlist(StrictStr)] = Field(default=..., alias="alternateNames")  # type: ignore
    color: Optional[StrictStr] = Field(...)
    alternate_color: Optional[StrictStr] = Field(default=..., alias="alternateColor")
    logos: Optional[conlist(StrictStr)] = Field(...)  # type: ignore
    twitter: Optional[StrictStr] = Field(...)
    # __properties = ["id", "mascot", "abbreviation", "alternateNames", "color", "alternateColor", "logos", "twitter"]

    @staticmethod
    def model_id() -> str:
        return "team_ext"

    @staticmethod
    def model_repository() -> Type[AbstractRepository]:
        from . import team_repository

        return team_repository.TeamExtRepository
