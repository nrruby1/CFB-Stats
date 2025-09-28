from typing import Optional, Union, Type
from pydantic import ConfigDict, Field, StrictInt, StrictFloat, StrictStr, StrictBool
from pydantic_mongo import AbstractRepository
from .cfb_model import CfbBaseModel

class Venue(CfbBaseModel):

    model_config = ConfigDict(validate_by_name = True, validate_assignment = True)
    
    venue_id: StrictInt = Field(default=..., alias="venueId")
    name: StrictStr = Field(...)
    city: Optional[StrictStr] = Field(...)
    state: Optional[StrictStr] = Field(...)
    zip: Optional[StrictStr] = Field(...)
    country_code: Optional[StrictStr] = Field(default=..., alias="countryCode")
    timezone: Optional[StrictStr] = Field(...)
    latitude: Optional[Union[StrictFloat, StrictInt]] = Field(...)
    longitude: Optional[Union[StrictFloat, StrictInt]] = Field(...)
    elevation: Optional[StrictStr] = Field(...)
    capacity: Optional[StrictInt] = Field(...)
    construction_year: Optional[StrictInt] = Field(default=..., alias="constructionYear")
    grass: Optional[StrictBool] = None
    dome: Optional[StrictBool] = None

    @staticmethod
    def model_id() -> str:
        return "venue"
    
    @staticmethod
    def model_repository() -> Type[AbstractRepository]:
        return VenueRepository

class VenueRepository(AbstractRepository[Venue]):
    class Meta:
        collection_name = Venue.model_id()

    def find_venue(self, venue_id: str):
        return self.find_one_by({"venue_id": venue_id})