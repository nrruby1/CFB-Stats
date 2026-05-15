from pydantic_mongo import AbstractRepository

from .conference import Conference


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
