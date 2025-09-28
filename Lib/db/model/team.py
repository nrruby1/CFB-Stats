from typing import Optional, Type
from pydantic import ConfigDict, Field, StrictInt, StrictStr, conlist
from pydantic_mongo import AbstractRepository
from .cfb_model import CfbBaseModel

class Team(CfbBaseModel):

    model_config = ConfigDict(validate_by_name = True, validate_assignment = True)

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
        return TeamRepository

class TeamRepository(AbstractRepository[Team]):
    class Meta:
        collection_name = Team.model_id()

    def find_team(self, year: int, team_id: int = None, school: str = None):
        if team_id is None and school is None:
            raise Exception("Either 'team_id' or 'school' argument must not be None")
        
        query = {"year": year}
        if team_id is not None:
            query["team_id"] = team_id
        if school is not None:
            query["school"] = school

        return self.find_one_by(query)

class TeamExt(CfbBaseModel):

    model_config = ConfigDict(validate_by_name = True, validate_assignment = True)
    
    team_id: StrictInt = Field(default=..., alias="teamId")
    year: StrictInt = Field(...)
    mascot: Optional[StrictStr] = Field(...)
    abbreviation: Optional[StrictStr] = Field(...)
    alternate_names: Optional[conlist(StrictStr)] = Field(default=..., alias="alternateNames") # type: ignore
    color: Optional[StrictStr] = Field(...)
    alternate_color: Optional[StrictStr] = Field(default=..., alias="alternateColor")
    logos: Optional[conlist(StrictStr)] = Field(...) # type: ignore
    twitter: Optional[StrictStr] = Field(...)
    # __properties = ["id", "mascot", "abbreviation", "alternateNames", "color", "alternateColor", "logos", "twitter"]

    @staticmethod
    def model_id() -> str:
        return "team_ext"
    
    @staticmethod
    def model_repository() -> Type[AbstractRepository]:
        return TeamExtRepository

class TeamExtRepository(AbstractRepository[TeamExt]):
    class Meta:
        collection_name = TeamExt.model_id()

    def find_team_ext(self, year: int, team_id: int):
        return self.find_one_by({"team_id": team_id, "year": year})
    
    def find_team_ext(self, team: Team):
        return self.find_one_by({"team_id": team.team_id, "year": team.year})