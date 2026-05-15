from typing import Optional, Type, override
from pydantic import ConfigDict, Field, StrictInt, StrictStr
from pydantic_mongo import AbstractRepository
from .cfb_model import CfbBaseModel


class Conference(CfbBaseModel):

    conference_id: StrictInt = Field(...)
    name: StrictStr = Field(...)
    classification: StrictStr = Field(...)
    short_name: Optional[StrictStr] = Field(...)
    abbreviation: Optional[StrictStr] = Field(...)

    @override
    def get_model_query(self) -> dict:
        return {"conference_id": self.conference_id}

    @override
    @staticmethod
    def model_id() -> str:
        return "conference"

    @override
    @staticmethod
    def model_repository() -> Type[AbstractRepository]:
        from . import conference_repository

        return conference_repository.ConferenceRepository

    @override
    def __eq__(self, value):
        if not isinstance(value, Conference):
            return False

        return self.conference_id == value.conference_id
