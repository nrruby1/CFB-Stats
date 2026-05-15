import logging

from db.db_connection import DbConnection, Databases, ExtractionCollections
from db.db_cleanup import *
from db.db_utility import *
from db.model.game import SeasonType
from etl.etls.etl import ExtractionDataSet
from etl.cfbd_connection import *

log = logging.getLogger("CfbStats.etl.datasets")


class ExtractConferenceDataSet(ExtractionDataSet):
    """Extract conferences from CFBD in a given list of classifications."""

    def __init__(self, class_list: list[str]):
        if class_list is None or len(class_list) == 0:
            raise ValueError("No classifications were specified")

        super().__init__()
        self.class_list = class_list

    def extract(self, cfbd_client, db_client, operations) -> bool:

        api = cfbd.ConferencesApi(cfbd_client)

        # Get conferences from API
        conferences = api_call(lambda: api.get_conferences())
        if conferences is None:
            log.warning("No data fetched from API")
            return True

        # Load conferences into extraction DB
        try:
            count = 0
            for conf in conferences:
                if conf.classification not in self.class_list:
                    continue

                op = insert_one_operation(
                    db_client=db_client,
                    db=Databases.extraction,
                    entity=conf.to_dict(),
                    model=ExtractionCollections.conference,
                    query={"id": conf.id},
                    do_replace=True,
                )

                if op is not None:
                    operations.append(op)
                    count += 1
                else:
                    log.warning(
                        f"Failed to create insert operation for conference id {conf.id}"
                    )

            log.debug(f"Extracted {count} entities")
        except Exception as e:
            log.exception(f"Exception when writing to DB: {e}")
            return False

        return True


class ExtractGamesDataSet(ExtractionDataSet):
    """Extract games from CFBD."""

    def __init__(
        self,
        year_list: list[int],
        class_list: list[str],
        week_list: list[int],
        season_types: list[SeasonType],
    ):
        if year_list is None or len(year_list) == 0:
            raise ValueError("No years were specified")
        if class_list is None or len(class_list) == 0:
            raise ValueError("No classifications were specified")
        if week_list is None or len(week_list) == 0:
            raise ValueError("No weeks were specified")
        if season_types is None or len(season_types) == 0:
            raise ValueError("No season types were specified")

        super().__init__()
        self.year_list = year_list
        self.class_list = class_list
        self.week_list = week_list
        self.season_types = season_types

    def extract(self, cfbd_client, db_client, operations) -> bool:
        api = cfbd.GamesApi(cfbd_client)

        count = 0
        games = []
        for year in self.year_list:
            games.extend(api_call(lambda: api.get_games(year=year)))

        if games is None or len(games) == 0:
            log.warning(f"No games data for years {self.year_list}")
            return True

        try:
            for game in games:
                if (
                    (
                        game.home_classification not in self.class_list
                        and game.away_classification not in self.class_list
                    )
                    or not any(
                        season_type.value == game.season_type
                        for season_type in self.season_types
                    )
                    or game.week not in self.week_list
                ):
                    continue

                op = insert_one_operation(
                    db_client=db_client,
                    db=Databases.extraction,
                    entity=game.to_dict(),
                    model=ExtractionCollections.game,
                    query={"id": game.id},
                    do_replace=True,
                )
                if op is not None:
                    operations.append(op)
                    count += 1
                else:
                    log.warning(
                        f"Failed to create insert operation for game id {game.id}"
                    )
        except Exception as e:
            log.exception(f"Exception when writing to DB: {e}")
            return False

        log.debug(f"Extracted {count} games")
        return True


class ExtractGameTeamStats(ExtractionDataSet):
    """Extract game team statistics from CFBD."""

    def __init__(
        self,
        year_list: list[int],
        week_list: list[int],
        season_types: list[SeasonType],
    ):
        if year_list is None or len(year_list) == 0:
            raise ValueError("No years were specified")
        if week_list is None or len(week_list) == 0:
            raise ValueError("No weeks were specified")
        if season_types is None or len(season_types) == 0:
            raise ValueError("No season types were specified")

        super().__init__()
        self.year_list = year_list
        self.week_list = week_list
        self.season_types = season_types

    def extract(self, cfbd_client, db_client, operations) -> bool:

        api = cfbd.GamesApi(cfbd_client)

        count = 0
        game_stats = []
        for year in self.year_list:
            for week in self.week_list:
                for season_type in self.season_types:
                    if season_type == SeasonType.POSTSEASON and week > 1:
                        # Postseason only has week 1 data
                        continue

                    # Get weekly game statistics from API
                    game_stats.extend(
                        api_call(
                            lambda: api.get_game_team_stats(
                                year=year, week=week, season_type=season_type.value
                            )
                        )
                    )

        if game_stats is None or len(game_stats) == 0:
            log.warning(
                f"No game stats data for years {self.year_list} weeks {self.week_list} season types {self.season_types}"
            )
            return True

        try:
            for stat in game_stats:

                op = insert_one_operation(
                    db_client=db_client,
                    db=Databases.extraction,
                    entity=stat.to_dict(),
                    model=ExtractionCollections.game_team_stats,
                    query={"id": stat.id},
                    do_replace=True,
                )
                if op is not None:
                    operations.append(op)
                    count += 1
                else:
                    log.warning(
                        f"Failed to create insert operation for stat id {stat.id}"
                    )
        except Exception as e:
            log.exception(f"Exception when writing to DB: {e}")
            return False

        log.debug(f"Extracted {count} entities")
        return True


class ExtractTeamDataSet(ExtractionDataSet):
    """Extract teams from CFBD in a given list of years and classifications."""

    def __init__(self, year_list: list[int], class_list: list[str]):
        if year_list is None or len(year_list) == 0:
            raise ValueError("No years were specified")
        if class_list is None or len(class_list) == 0:
            raise ValueError("No classifications were specified")

        super().__init__()
        self.year_list = year_list
        self.class_list = class_list

    def extract(
        self, cfbd_client: CfbdConnection, db_client: DbConnection, operations
    ) -> bool:

        api = cfbd.TeamsApi(cfbd_client)
        for year in self.year_list:

            # Get teams from API
            teams = api_call(lambda: api.get_teams(year=year))
            if teams == None:
                log.warning("No data fetched from API")
                return True

            # Load teams into extraction DB
            try:
                count = 0
                for team in teams:
                    if team.classification not in self.class_list:
                        continue
                    team_dict = team.to_dict()
                    team_dict["year"] = year
                    op = insert_one_operation(
                        db_client=db_client,
                        db=Databases.extraction,
                        entity=team_dict,
                        model=ExtractionCollections.team,
                        query={"id": team.id, "year": year},
                        do_replace=True,
                    )
                    if op is not None:
                        operations.append(op)
                        count += 1
                    else:
                        log.warning(
                            f"Failed to create insert operation for team id {team.id} year {year}"
                        )
                log.debug(f"Extracted {count} entities")
            except Exception as e:
                log.exception(f"Exception when writing to DB: {e}")
                return False

        return True


class ExtractVenueDataSet(ExtractionDataSet):
    """Extract venue from CFBD."""

    def __init__(self):
        super().__init__()

    def extract(self, cfbd_client, db_client, operations) -> bool:
        api = cfbd.VenuesApi(cfbd_client)

        # Get venue from API
        venues = api_call(lambda: api.get_venues())
        if venues == None:
            log.warning("No data fetched from API")
            return True

        # Load venues into extraction DB
        try:
            count = 0
            for venue in venues:
                op = insert_one_operation(
                    db_client=db_client,
                    db=Databases.extraction,
                    entity=venue.to_dict(),
                    model=ExtractionCollections.venue,
                    query={"id": venue.id},
                    do_replace=True,
                )
                if op is not None:
                    operations.append(op)
                    count += 1
                else:
                    log.warning(
                        f"Failed to create insert operation for venue id {venue.id}"
                    )
            log.debug(f"Extracted {count} entities")
        except Exception as e:
            log.exception(f"Exception when writing to DB: {e}")
            return False

        return True
