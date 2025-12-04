import logging

from etl.etl import ExtractionDataSet
from db.db_connection import DbConnection, Databases, ExtractionCollections
from db.db_cleanup import *
from cfbd_connection import *

log = logging.getLogger('CfbStats.etl')

class ExtractTeamDataSet(ExtractionDataSet):
    """Extract teams from CFBD in a given list of years and classifications."""

    def __init__(self, year_list: list[int], class_list: list[str]):
        super().__init__()
        self.year_list = year_list
        self.class_list = class_list
    
    def extract(self, cfbd_client: CfbdConnection, db_client: DbConnection) -> bool:
        if self.year_list == None or len(self.year_list) == 0:
            log.warning("ExtractTeamDataSet: Skipping since no years were specified")
            return True
        if self.class_list == None or len(self.class_list) == 0:
            log.warning("ExtractTeamDataSet: Skipping since no classifications were specified")
            return True
        
        api = cfbd.TeamsApi(cfbd_client)
        for year in self.year_list:

            # Get teams from API
            teams = api_call(lambda : api.get_teams(year=year))
            if teams == None:
                log.warning("ExtractTeamDataSet: No data fetched from API")
                return True
            
            # Load teams into extraction DB
            try:
                team_coll = db_client.get_cfb_collection(Databases.extraction, ExtractionCollections.team)
                count = 0
                for team in teams:
                    if team.classification not in self.class_list:
                        continue
                    team_dict = team.to_dict()
                    team_dict["year"] = year
                    query = {"id": team.id, "year": year}
                    team_coll.replace_one(query, team_dict, upsert=True)
                    count += 1
                log.info(f"ExtractTeamDataSet: Extracted {count} entities")
            except Exception as e:
                log.exception(f"ExtractTeamDataSet: Exception when writing to DB: {e}")
                return False
            
        return True

    def cleanup(self, db_client):
        try:
            cleanup_extraction_collections(db_client, ExtractionCollections.team)
        except Exception as e:
            log.exception(f"ExtractTeamDataSet: Exception when cleaning up: {e}")
            
class ExtractConferenceDataSet(ExtractionDataSet):
    """Extract conferences from CFBD in a given list of classifications."""

    def __init__(self, class_list: list[str]):
        super().__init__()
        self.class_list = class_list

    def extract(self, cfbd_client, db_client) -> bool:  
        if self.class_list == None or len(self.class_list) == 0:
            return True
              
        api = cfbd.ConferencesApi(cfbd_client)

        # Get conferences from API
        conferences = api_call(lambda : api.get_conferences())
        if conferences == None:
            log.warning("ExtractConferenceDataSet: No data fetched from API")
            return True
        
        # Load conferences into extraction DB
        try:
            conf_coll = db_client.get_cfb_collection(Databases.extraction, ExtractionCollections.conference)
            count = 0
            for conf in conferences:
                if conf.classification not in self.class_list:
                    continue
                conf_dict = conf.to_dict()
                query = {"id": conf.id}
                conf_coll.replace_one(query, conf_dict, upsert=True)
                count += 1
            log.info(f"ExtractConferenceDataSet: Extracted {count} entities")
        except Exception as e:
            log.exception(f"ExtractConferenceDataSet: Exception when writing to DB: {e}")
            return False

        return True

    def cleanup(self, db_client):
        try:
            cleanup_extraction_collections(db_client, ExtractionCollections.conference)
        except Exception as e:
            log.exception(f"ExtractConferenceDataSet: Exception when cleaning up: {e}")

class ExtractVenueDataSet(ExtractionDataSet):
    """Extract venue from CFBD."""

    def __init__(self):
        super().__init__()

    def extract(self, cfbd_client, db_client) -> bool:
        api = cfbd.VenuesApi(cfbd_client)

        # Get venue from API
        venues = api_call(lambda : api.get_venues())
        if venues == None:
            log.warning("ExtractVenueDataSet: No data fetched from API")
            return True
        
        # Load venues into extraction DB
        try:
            venue_coll = db_client.get_cfb_collection(Databases.extraction, ExtractionCollections.venue)
            count = 0
            for venue in venues:
                venue_dict = venue.to_dict()
                query = {"id": venue.id}
                venue_coll.replace_one(query, venue_dict, upsert=True)
                count += 1
            log.info(f"ExtractVenueDataSet: Extracted {count} entities")
        except Exception as e:
            log.exception(f"ExtractVenueDataSet: Exception when writing to DB: {e}")
            return False

        return True

    def cleanup(self, db_client):
        try:
            cleanup_extraction_collections(db_client, ExtractionCollections.venue)
        except Exception as e:
            log.exception(f"ExtractVenueDataSet: Exception when cleaning up: {e}")

class ExtractGamesDataSet(ExtractionDataSet):
    """Extract games from CFBD."""

    def __init__(self, year_list: list[int], class_list: list[str], week_list: list[int] = None):
        super().__init__()
        self.year_list = year_list
        self.class_list = class_list
        self.week_list = week_list

    def extract(self, cfbd_client, db_client) -> bool:
        api = cfbd.GamesApi(cfbd_client)

        count = 0
        for year in self.year_list:
            games = api_call(lambda : api.get_games(year=year))

            if games is None or len(games) == 0:
                log.warning(f"ExtractGamesDataSet: No games data for year {year}")
                continue

            try:
                games_coll = db_client.get_cfb_collection(Databases.extraction, ExtractionCollections.game)
                for game in games:
                    if game.home_classification not in self.class_list and game.away_classification not in self.class_list:
                        continue
                    if self.week_list is not None and game.week not in self.week_list:
                        continue

                    game_dict = game.to_dict()
                    query = {"id": game.id}
                    games_coll.replace_one(query, game_dict, upsert=True)
                    count += 1
            except Exception as e:
                log.exception(f"ExtractGamesDataSet: Exception when writing to DB: {e}")
                return False
        
        log.info(f"ExtractGamesDataSet: Extracted {count} games")
        return True

    def cleanup(self, db_client):
        try:
            cleanup_extraction_collections(db_client, ExtractionCollections.game)
        except Exception as e:
            log.exception(f"ExtractGamesDataSet: Exception when cleaning up: {e}")