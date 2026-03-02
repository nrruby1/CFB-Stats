from pydantic_mongo import AbstractRepository

from .team import Team, TeamExt


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


class TeamExtRepository(AbstractRepository[TeamExt]):
    class Meta:
        collection_name = TeamExt.model_id()

    def find_team_ext(self, year: int, team_id: int):
        return self.find_one_by({"team_id": team_id, "year": year})

    def find_team_ext(self, team: Team):
        return self.find_one_by({"team_id": team.team_id, "year": team.year})
