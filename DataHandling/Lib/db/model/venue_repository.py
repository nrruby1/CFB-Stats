from pydantic_mongo import AbstractRepository

from .venue import Venue


class VenueRepository(AbstractRepository[Venue]):
    class Meta:
        collection_name = Venue.model_id()

    def find_venue(self, venue_id: int):
        return self.find_one_by({"venue_id": venue_id})
