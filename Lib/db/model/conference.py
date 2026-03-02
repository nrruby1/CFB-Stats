from typing import Optional, Type
from pydantic import ConfigDict, Field, StrictInt, StrictStr
from pydantic_mongo import AbstractRepository
from .cfb_model import CfbBaseModel


class Conference(CfbBaseModel):

    model_config = ConfigDict(validate_by_name=True, validate_assignment=True)

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
        from . import conference_repository

        return conference_repository.ConferenceRepository
