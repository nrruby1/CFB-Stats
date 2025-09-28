from typing import Optional, Type
from pydantic import ConfigDict, Field, StrictInt, StrictStr
from pydantic_mongo import AbstractRepository
from .cfb_model import CfbBaseModel

class Conference(CfbBaseModel):

    model_config = ConfigDict(validate_by_name = True, validate_assignment = True)

    conference_id: StrictInt = Field(default=..., alias="conferenceId")
    name: StrictStr = Field(...)
    classification: StrictStr = Field(...)
    shortName: Optional[StrictStr] = Field(...)
    abbreviation: Optional[StrictStr] = Field(...)

    @staticmethod
    def model_id() -> str:
        return "conference"
    
    @staticmethod
    def model_repository() -> Type[AbstractRepository]:
        return ConferenceRepository

class ConferenceRepository(AbstractRepository[Conference]):
    class Meta:
        collection_name = Conference.model_id()

    def find_conference(self, conference_id: int = None, name: str = None):
        if conference_id is None and name is None:
            raise Exception("Either 'conference_id' or 'name' argument must not me None")
        
        query = dict()
        if conference_id is not None:
            query["conference_id"] = conference_id
        if name is not None:
            query["name"] = name

        return self.find_one_by(query)