import logging
from webbrowser import get

from etl.etl import *
from db.db_connection import DbConnection, Databases, ExtractionCollections
from db.db_cleanup import *
from db.model.team import *
from db.model.conference import *
from db.model.venue import *
from etl.extraction_datasets import ExtractTeamDataSet, ExtractConferenceDataSet, ExtractVenueDataSet

class EtlInit(EtlBase):
    
    def __init__(self, *, log: logging.Logger = None, clean_extract: bool = True, clean_staging: bool = True):
        if log == None:
            log = logging.getLogger(type(self).__name__)

        super().__init__(log, clean_extract=clean_extract, clean_staging=clean_staging)

        years = [2023, 2024, 2025]
        classifications = ["fbs", "fcs"]

        self.datasets = {
            InitDataset(log, years, classifications)
        }

    def post_transform(self) -> bool:
        return True

    def validate(self) -> bool:
        return True

class InitDataset(DataSet):
    def __init__(self, log, years: list[int], classifications: list[str]):
        super().__init__(log)
        self.classifications = classifications

        self.extract_datasets = {
            ExtractTeamDataSet(log, year_list=years, class_list=classifications), 
            ExtractConferenceDataSet(log, class_list=classifications),
            ExtractVenueDataSet(log)
        }

    def transform(self, db_client) -> bool:
        try: 
            extr_team_coll = db_client.get_cfb_collection(Databases.extraction, ExtractionCollections.team)
            stage_team_repo: TeamRepository = db_client.get_cfb_repository(Databases.staging, Team)
            stage_team_ext_repo: TeamExtRepository = db_client.get_cfb_repository(Databases.staging, TeamExt)

            extr_teams = extr_team_coll.find()
            for extr_team in extr_teams:
                validate_fields = validate_mandatory_fields(extr_team, "id", "year", "school", "conference", 
                                                           "classification", "location")
                if not validate_fields:
                    self.log.warning(f"InitDataset: Skipping {extr_team.get("school")} due to missing mandatory field(s)")
                    continue

                if extr_team["classification"] not in self.classifications:
                    continue
                
                conference = self.get_or_create_conference(db_client, extr_team)
                if conference is None:
                    self.log.warning(f"InitDataset: {extr_team.get("school")} has no conference, skipping")
                    continue

                venue = self.get_or_create_venue(db_client, extr_team)
                if venue is None:
                    self.log.warning(f"InitDataset: {extr_team.get("school")} has no venue")

                team = Team(
                    teamId=extr_team.get("id"),
                    year=extr_team.get("year"),
                    school=extr_team.get("school"),
                    conferenceId=conference.conference_id,
                    classification=extr_team.get("classification"),
                    division=extr_team.get("division"),
                    venueId=None
                )
                if venue is not None:
                    team.venue_id=venue.venue_id

                Team.model_validate(team)
                stage_team_repo.save(team)

                team_ext = TeamExt(
                    teamId=extr_team.get("id"),
                    year=extr_team.get("year"),
                    mascot=extr_team.get("mascot"),
                    abbreviation=extr_team.get("abbreviation"),
                    alternateNames=extr_team.get("alternateNames"),
                    color=extr_team.get("color"),
                    alternateColor=extr_team.get("alternateColor"),
                    logos=extr_team.get("logos"),
                    twitter=extr_team.get("twitter")
                )

                TeamExt.model_validate(team_ext)
                stage_team_ext_repo.save(team_ext)

        except Exception as e:
            self.log.exception("InitDataset: Exception when transforming: %s" % e)
            return False
        
        return True

    def get_or_create_conference(self, db_client: DbConnection, extr_team: dict) -> Conference | None:
        stage_conference_repo: ConferenceRepository = db_client.get_cfb_repository(Databases.staging, Conference)
        extr_conference_coll = db_client.get_cfb_collection(Databases.extraction, ExtractionCollections.conference)

        conference = stage_conference_repo.find_conference(name=extr_team.get("conference"))
        if conference is not None:
            return conference
        
        query = {"name": extr_team.get("conference"), "classification": extr_team.get("classification")}
        extr_conference = extr_conference_coll.find_one(query)
        if extr_conference is None:
            return None
        
        if not validate_mandatory_fields(extr_conference, "id", "name", "classification"):
            return None

        conference = Conference(
            conferenceId=extr_conference.get("id"),
            name=extr_conference.get("name"),
            shortName=extr_conference.get("shortName"),
            abbreviation=extr_conference.get("abbreviation"),
            classification=extr_conference.get("classification")
        )

        Conference.model_validate(conference)
        stage_conference_repo.save(conference)
        return conference
    
    def get_or_create_venue(self, db_client: DbConnection, extr_team: dict) -> Venue | None:
        if not validate_mandatory_fields(extr_team, "location"):
            return None
        stage_venue_repo: VenueRepository = db_client.get_cfb_repository(Databases.staging, Venue)
        extr_venue_coll = db_client.get_cfb_collection(Databases.extraction, ExtractionCollections.venue)

        venue = stage_venue_repo.find_venue(extr_team.get("location").get("id"))
        if venue is not None:
            return venue
        
        query = {"id": extr_team.get("location").get("id")}
        extr_venue = extr_venue_coll.find_one(query)
        if extr_venue is None:
            return None
        
        if not validate_mandatory_fields(extr_venue, "id", "name"):
            return None
        
        venue = Venue(
            venueId=extr_venue.get("id"),
            name=extr_venue.get("name"),
            city=extr_venue.get("city"),
            state=extr_venue.get("state"),
            zip=extr_venue.get("zip"),
            countryCode=extr_venue.get("countryCode"),
            timezone=extr_venue.get("timezone"),
            latitude=extr_venue.get("latitude"),
            longitude=extr_venue.get("longitude"),
            elevation=extr_venue.get("elevation"),
            capacity=extr_venue.get("capacity"),
            constructionYear=extr_venue.get("constructionYear"),
            grass=extr_venue.get("grass"),
            dome=extr_venue.get("dome")
        )

        Venue.model_validate(venue)
        stage_venue_repo.save(venue)
        return venue

    def load(self, db_client):
        try:
            stage_team_repo: TeamRepository = db_client.get_cfb_repository(Databases.staging, Team)
            stage_team_ext_repo: TeamExtRepository = db_client.get_cfb_repository(Databases.staging, TeamExt)
            stage_conference_repo: ConferenceRepository = db_client.get_cfb_repository(Databases.staging, Conference)
            stage_venue_repo: VenueRepository = db_client.get_cfb_repository(Databases.staging, Venue)
            
            prod_team_repo: TeamRepository = db_client.get_cfb_repository(Databases.production, Team)
            prod_team_ext_repo: TeamExtRepository = db_client.get_cfb_repository(Databases.production, TeamExt)
            prod_conference_repo: ConferenceRepository = db_client.get_cfb_repository(Databases.production, Conference)
            prod_venue_repo: VenueRepository = db_client.get_cfb_repository(Databases.production, Venue)

            query = lambda entity : {"year": entity.year, "team_id": entity.team_id}
            load_into_production(prod_repo=prod_team_repo, stage_repo=stage_team_repo, query=query)

            query = lambda entity : {"year": entity.year, "team_id": entity.team_id}
            load_into_production(prod_repo=prod_team_ext_repo, stage_repo=stage_team_ext_repo, query=query)

            query = lambda entity : {"conference_id": entity.conference_id}
            load_into_production(prod_repo=prod_conference_repo, stage_repo=stage_conference_repo, query=query)

            query = lambda entity : {"venue_id": entity.venue_id}
            load_into_production(prod_repo=prod_venue_repo, stage_repo=stage_venue_repo, query=query)
        except Exception as e:
            self.log.exception("InitDataset: Exception when loading: %s" % e)
    
    def cleanup(self, db_client):
        try:
            cleanup_staging_collections(db_client, Team, TeamExt, Conference, Venue)
        except Exception as e:
            self.log.exception("InitDataset: Exception when during cleanup: %s" % e)