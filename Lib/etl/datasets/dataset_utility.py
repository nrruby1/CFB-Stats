from db.db_connection import *
from db.model.conference import Conference
from db.model.venue import Venue
from etl.etls.etl import *


def get_or_create_conference(
    db_client: DbConnection,
    conference_name: str,
    classification: str,
    count: int = None,
) -> Conference | None:
    """
    Get the conference from the staging or production database, or create it from the extraction database.
    """
    extr_conference_coll = db_client.get_cfb_collection(
        Databases.extraction, ExtractionCollections.conference
    )
    stage_conference_repo, prod_conference_repo = get_repos(db_client, Conference)

    conference: Conference | None = stage_conference_repo.find_conference(
        name=conference_name
    )
    if conference is not None:
        return conference

    conference = prod_conference_repo.find_conference(name=conference_name)
    if conference is not None:
        return conference

    query = {"name": conference_name, "classification": classification}
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
        classification=extr_conference.get("classification"),
    )

    Conference.model_validate(conference)
    stage_conference_repo.save(conference)
    if count is not None:
        count += 1

    return conference


def get_or_create_venue(
    db_client: DbConnection, venue_id: int, count: int = None
) -> Venue | None:
    """
    Get the venue from the staging or production database, or create it from the extraction database.
    """
    extr_venue_coll = db_client.get_cfb_collection(
        Databases.extraction, ExtractionCollections.venue
    )
    stage_venue_repo, prod_venue_repo = get_repos(db_client, Venue)

    venue: Venue | None = stage_venue_repo.find_venue(venue_id)
    if venue is not None:
        return venue

    venue = prod_venue_repo.find_venue(venue_id)
    if venue is not None:
        return venue

    query = {"id": venue_id}
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
        dome=extr_venue.get("dome"),
    )

    Venue.model_validate(venue)
    stage_venue_repo.save(venue)
    if count is not None:
        count += 1

    return venue


def get_game(db_client: DbConnection, game_id: int) -> Game | None:
    """
    Get a game from the staging or production database.
    """
    stage_game_repo, prod_game_repo = get_repos(db_client, Game)

    game: Game | None = stage_game_repo.find_game(game_id)
    if game is not None:
        return game

    game = prod_game_repo.find_game(game_id)
    if game is not None:
        return game

    return None
