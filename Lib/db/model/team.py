from typing import Optional, Type, override
from pydantic import ConfigDict, Field, StrictInt, StrictStr, conlist
from pydantic_mongo import AbstractRepository
from .cfb_model import CfbBaseModel


class Team(CfbBaseModel):

    team_id: StrictInt = Field(...)
    year: StrictInt = Field(...)
    school: StrictStr = Field(...)
    conference_id: StrictInt = Field(...)
    classification: StrictStr = Field(...)
    division: Optional[StrictStr] = Field(...)
    venue_id: Optional[StrictInt] = Field(...)

    @override
    def get_model_query(self) -> dict:
        return {"team_id": self.team_id, "year": self.year}

    @override
    @staticmethod
    def model_id() -> str:
        return "team"

    @staticmethod
    def model_repository() -> Type[AbstractRepository]:
        from . import team_repository

        return team_repository.TeamRepository

    @override
    def __eq__(self, value):
        if not isinstance(value, Team):
            return False

        return self.team_id == value.team_id and self.year == value.year


class TeamExt(CfbBaseModel):

    team_id: StrictInt = Field(...)
    year: StrictInt = Field(...)
    mascot: Optional[StrictStr] = Field(...)
    abbreviation: Optional[StrictStr] = Field(...)
    alternate_names: Optional[conlist(StrictStr)] = Field(...)  # type: ignore
    color: Optional[StrictStr] = Field(...)
    alternate_color: Optional[StrictStr] = Field(...)
    logos: Optional[conlist(StrictStr)] = Field(...)  # type: ignore
    twitter: Optional[StrictStr] = Field(...)

    @override
    def get_model_query(self) -> dict:
        return {"team_id": self.team_id, "year": self.year}

    @override
    @staticmethod
    def model_id() -> str:
        return "team_ext"

    @override
    @staticmethod
    def model_repository() -> Type[AbstractRepository]:
        from . import team_repository

        return team_repository.TeamExtRepository

    @override
    def __eq__(self, value):
        if not isinstance(value, TeamExt):
            return False

        return self.team_id == value.team_id and self.year == value.year
