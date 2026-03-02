from db.db_connection import Databases, ExtractionCollections
from db.db_cleanup import *
from db.model.team import *
from db.model.team_repository import *
from db.model.conference import *
from db.model.venue import *
from etl.etls.etl import *
from etl.datasets.dataset_utility import *
from etl.datasets.extraction_datasets import *

log = logging.getLogger("CfbStats.etl")


class TeamDataset(DataSet):
    """
    Transfers team, conference, and venue data from a given list of years and conference classifications.
    """

    def __init__(self, years: list[int], classifications: list[str]):
        super().__init__()
        self.classifications = classifications

        self.extract_datasets = {
            ExtractTeamDataSet(year_list=years, class_list=classifications),
            ExtractConferenceDataSet(class_list=classifications),
            ExtractVenueDataSet(),
        }

    def transform(self, db_client) -> bool:
        try:
            extr_team_coll: Collection = db_client.get_cfb_collection(
                Databases.extraction, ExtractionCollections.team
            )
            stage_team_repo: TeamRepository = db_client.get_cfb_repository(
                Databases.staging, Team
            )
            stage_team_ext_repo: TeamExtRepository = db_client.get_cfb_repository(
                Databases.staging, TeamExt
            )

            count = 0
            extr_teams = extr_team_coll.find()
            for extr_team in extr_teams:
                validate_fields = validate_mandatory_fields(
                    extr_team,
                    "id",
                    "year",
                    "school",
                    "conference",
                    "classification",
                    "location",
                )
                if not validate_fields:
                    log.warning(
                        f"TeamDataset: Skipping {extr_team.get("school")} due to missing mandatory field(s)"
                    )
                    continue

                if extr_team["classification"] not in self.classifications:
                    continue

                conference = get_or_create_conference(
                    db_client,
                    extr_team.get("conference"),
                    extr_team.get("classification"),
                    count,
                )
                if conference is None:
                    log.warning(
                        f"TeamDataset: {extr_team.get('school')} has no conference, skipping"
                    )
                    continue

                venue = get_or_create_venue(
                    db_client, extr_team.get("location").get("id"), count
                )
                if venue is None:
                    log.warning(f"TeamDataset: {extr_team.get('school')} has no venue")

                team = Team(
                    teamId=extr_team.get("id"),
                    year=extr_team.get("year"),
                    school=extr_team.get("school"),
                    conferenceId=conference.conference_id,
                    classification=extr_team.get("classification"),
                    division=extr_team.get("division"),
                    venueId=None,
                )
                if venue is not None:
                    team.venue_id = venue.venue_id

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
                    twitter=extr_team.get("twitter"),
                )

                TeamExt.model_validate(team_ext)
                stage_team_ext_repo.save(team_ext)
                count += 1

            log.debug(f"TeamDataset: Transformed {count} entities")
            return True
        except Exception as e:
            log.exception("TeamDataset: Exception when transforming: %s" % e)
            return False

    def load(self, db_client):
        try:
            stage_team_repo, prod_team_repo = get_repos(db_client, Team)
            stage_team_ext_repo, prod_team_ext_repo = get_repos(db_client, TeamExt)
            stage_venue_repo, prod_venue_repo = get_repos(db_client, Venue)
            stage_conference_repo, prod_conference_repo = get_repos(
                db_client, Conference
            )

            query = lambda entity: {"year": entity.year, "team_id": entity.team_id}
            load_into_production(
                prod_repo=prod_team_repo, stage_repo=stage_team_repo, query=query
            )

            query = lambda entity: {"year": entity.year, "team_id": entity.team_id}
            load_into_production(
                prod_repo=prod_team_ext_repo,
                stage_repo=stage_team_ext_repo,
                query=query,
            )

            query = lambda entity: {"conference_id": entity.conference_id}
            load_into_production(
                prod_repo=prod_conference_repo,
                stage_repo=stage_conference_repo,
                query=query,
            )

            query = lambda entity: {"venue_id": entity.venue_id}
            load_into_production(
                prod_repo=prod_venue_repo, stage_repo=stage_venue_repo, query=query
            )
        except Exception as e:
            log.exception("TeamDataset: Exception when loading: %s" % e)

    def cleanup(self, db_client):
        try:
            cleanup_staging_collections(db_client, Team, TeamExt, Conference, Venue)
        except Exception as e:
            log.exception("TeamDataset: Exception when during cleanup: %s" % e)
