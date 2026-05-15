from typing import Type
from pydantic_mongo import AbstractRepository

from .team import Team
from .game import Game, GameTeamStats


class GameRepository(AbstractRepository[Game]):
    class Meta:
        collection_name = Game.model_id()

    def find_game(self, game_id: int):
        return self.find_one_by({"game_id": game_id})


class GameTeamStatsRepository(AbstractRepository[GameTeamStats]):
    class Meta:
        collection_name = GameTeamStats.model_id()

    def find_game_team_stats(
        self,
        game: Game = None,
        game_id: int = None,
        team: Team = None,
        team_id: int = None,
    ) -> GameTeamStats | None:

        if game is None and game_id is None:
            raise Exception("Either 'game' or 'game_id' argument must not be None")
        if team is None and team_id is None:
            raise Exception("Either 'team' or 'team_id' argument must not be None")

        return self.find_one_by(
            {
                "game_id": game.game_id if game is not None else game_id,
                "team_id": team.team_id if team is not None else team_id,
            }
        )

    def find_game_stats(self, game_id: int):
        return self.find_by({"game_id": game_id})
