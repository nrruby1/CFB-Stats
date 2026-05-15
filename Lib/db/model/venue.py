from typing import Optional, Union, Type, override
from pydantic import ConfigDict, Field, StrictInt, StrictFloat, StrictStr, StrictBool
from pydantic_mongo import AbstractRepository
from .cfb_model import CfbBaseModel


class Venue(CfbBaseModel):

    venue_id: StrictInt = Field(...)
    name: StrictStr = Field(...)
    city: Optional[StrictStr] = Field(...)
    state: Optional[StrictStr] = Field(...)
    zip: Optional[StrictStr] = Field(...)
    country_code: Optional[StrictStr] = Field(...)
    timezone: Optional[StrictStr] = Field(...)
    latitude: Optional[Union[StrictFloat, StrictInt]] = Field(...)
    longitude: Optional[Union[StrictFloat, StrictInt]] = Field(...)
    elevation: Optional[StrictStr] = Field(...)
    capacity: Optional[StrictInt] = Field(...)
    construction_year: Optional[StrictInt] = Field(...)
    grass: Optional[StrictBool] = None
    dome: Optional[StrictBool] = None

    @override
    def get_model_query(self) -> dict:
        return {"venue_id": self.venue_id}

    @override
    @staticmethod
    def model_id() -> str:
        return "venue"

    @override
    @staticmethod
    def model_repository() -> Type[AbstractRepository]:
        from . import venue_repository

        return venue_repository.VenueRepository

    @override
    def __eq__(self, value):
        if not isinstance(value, Venue):
            return False

        return self.venue_id == value.venue_id
