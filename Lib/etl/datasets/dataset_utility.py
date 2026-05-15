from db.db_connection import *
from db.db_utility import *
from db.model.conference import Conference
from db.model.venue import Venue


def get_repos(
    db_client: DbConnection,
    model: type[CfbBaseModel],
) -> tuple[AbstractRepository, AbstractRepository]:
    """
    Helper function to return the staging and production repositories for a given model, respectively.
    """

    stage_repo: AbstractRepository = db_client.get_cfb_repository(
        Databases.staging, model
    )
    prod_repo: AbstractRepository = db_client.get_cfb_repository(
        Databases.production, model
    )

    return stage_repo, prod_repo


def validate_mandatory_fields(entity, *fields) -> bool:
    for field in fields:
        if field not in entity or entity[field] is None:
            return False

        if type(entity[field]) is str and entity[field] == "":
            return False

        if type(entity[field]) in (dict, list, set, tuple) and len(entity[field]) == 0:
            return False

    return True


def get_or_create_conference(
    db_client: DbConnection,
    conference_name: str,
    classification: str,
    operations: list,
    count: int = None,
) -> Optional[Conference]:
    """
    Get the conference from the staging or production database, or create it from the extraction database.
    """
    extr_conference_coll = db_client.get_cfb_collection(
        Databases.extraction, ExtractionCollections.conference
    )
    prod_conference_repo = db_client.get_cfb_repository(
        Databases.production, Conference
    )

    conference = get_insert_operation_from_list(
        operations=operations,
        query={"name": conference_name},
        namespace=db_client.get_collection_namespace(Databases.staging, Conference),
    )
    if conference is not None:
        return Conference.model_construct(**conference)

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
        conference_id=extr_conference.get("id"),
        name=extr_conference.get("name"),
        short_name=extr_conference.get("shortName"),
        abbreviation=extr_conference.get("abbreviation"),
        classification=extr_conference.get("classification"),
    )

    Conference.model_validate(conference)
    op = insert_one_operation(
        db_client=db_client,
        db=Databases.staging,
        entity=conference,
        do_replace=False,
    )
    if op is not None:
        operations.append(op)
        count += 1
        return conference
    else:
        log.warning(
            f"TeamDataset: Failed to create insert operation for conference {conference_name}"
        )

    return None


def get_or_create_venue(
    db_client: DbConnection, venue_id: int, operations: list, count: int = None
) -> Optional[Venue]:
    """
    Get the venue from the staging or production database, or create it from the extraction database.
    """
    extr_venue_coll = db_client.get_cfb_collection(
        Databases.extraction, ExtractionCollections.venue
    )
    prod_venue_repo = db_client.get_cfb_repository(Databases.production, Venue)

    venue = get_insert_operation_from_list(
        operations=operations,
        query={"venue_id": venue_id},
        namespace=db_client.get_collection_namespace(Databases.staging, Venue),
    )
    if venue is not None:
        return Venue.model_construct(**venue)

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
        venue_id=extr_venue.get("id"),
        name=extr_venue.get("name"),
        city=extr_venue.get("city"),
        state=extr_venue.get("state"),
        zip=extr_venue.get("zip"),
        country_code=extr_venue.get("countryCode"),
        timezone=extr_venue.get("timezone"),
        latitude=extr_venue.get("latitude"),
        longitude=extr_venue.get("longitude"),
        elevation=extr_venue.get("elevation"),
        capacity=extr_venue.get("capacity"),
        construction_year=extr_venue.get("constructionYear"),
        grass=extr_venue.get("grass"),
        dome=extr_venue.get("dome"),
    )

    Venue.model_validate(venue)
    op = insert_one_operation(
        db_client=db_client,
        db=Databases.staging,
        entity=venue,
        do_replace=False,
    )
    if op is not None:
        operations.append(op)
        count += 1
        return venue
    else:
        log.warning(
            f"TeamDataset: Failed to create insert operation for venue {venue_id}"
        )

    return None


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
