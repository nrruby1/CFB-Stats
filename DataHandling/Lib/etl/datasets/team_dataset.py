from db.db_connection import Databases, ExtractionCollections
from db.db_cleanup import *
from db.db_utility import *
from db.model.team import *
from db.model.team_repository import *
from db.model.conference import *
from db.model.venue import *
from etl.etls.etl import *
from etl.datasets.dataset_utility import *
from etl.datasets.extraction_datasets import *

log = logging.getLogger("CfbStats.etl.datasets")


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

        self.models = {Team: False, TeamExt: False, Conference: False, Venue: False}

    def transform(self, db_client, operations) -> bool:
        try:
            extr_team_coll: Collection = db_client.get_cfb_collection(
                Databases.extraction, ExtractionCollections.team
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
                    operations,
                    count,
                )
                if conference is None:
                    log.warning(
                        f"TeamDataset: {extr_team.get('school')} has no conference, skipping"
                    )
                    continue

                venue = get_or_create_venue(
                    db_client, extr_team.get("location").get("id"), operations, count
                )
                if venue is None:
                    log.warning(f"TeamDataset: {extr_team.get('school')} has no venue")

                team = Team(
                    team_id=extr_team.get("id"),
                    year=extr_team.get("year"),
                    school=extr_team.get("school"),
                    conference_id=conference.conference_id,
                    classification=extr_team.get("classification"),
                    division=extr_team.get("division"),
                    venue_id=None,
                )
                if venue is not None:
                    team.venue_id = venue.venue_id

                team_ext = TeamExt(
                    team_id=extr_team.get("id"),
                    year=extr_team.get("year"),
                    mascot=extr_team.get("mascot"),
                    abbreviation=extr_team.get("abbreviation"),
                    alternate_names=extr_team.get("alternateNames"),
                    color=extr_team.get("color"),
                    alternate_color=extr_team.get("alternateColor"),
                    logos=extr_team.get("logos"),
                    twitter=extr_team.get("twitter"),
                )

                Team.model_validate(team)
                TeamExt.model_validate(team_ext)
                ops = insert_many_operations(
                    db_client=db_client,
                    db=Databases.staging,
                    entities=(team, team_ext),
                    do_replace=False,
                )
                if len(ops) == 2:
                    operations.extend(ops)
                    count += 2
                else:
                    log.warning(
                        f"TeamDataset: Failed to create insert operations for team {extr_team.get('school')}"
                    )

            log.debug(f"TeamDataset: Transformed {count} entities")
            return True
        except Exception as e:
            log.exception("TeamDataset: Exception when transforming: %s" % e)
            return False
