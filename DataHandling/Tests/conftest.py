import pytest
import json

import Tests.testing_logging_config

from db.db_connection import *

from db.db_cleanup import *

log = logging.getLogger("CfbStats.Tests.conftest")


@pytest.fixture(scope="session")
def db_client():
    with DbConnection(True) as client:
        yield client


@pytest.fixture(scope="module")
def cleanup_db(db_client: DbConnection):
    log.info("Cleaning up database before tests")
    cleanup_extraction_collections(db_client)
    cleanup_staging_collections(db_client)
    cleanup_production_collections(db_client)

    yield

    log.info("Cleaning up database after tests")
    cleanup_extraction_collections(db_client)
    cleanup_staging_collections(db_client)
    cleanup_production_collections(db_client)


@pytest.fixture(scope="function")
def cleanup_staging(db_client: DbConnection):
    yield
    log.info("Cleaning up staging collections")
    cleanup_staging_collections(db_client)


@pytest.fixture(scope="function")
def cleanup_production(db_client: DbConnection):
    yield
    log.info("Cleaning up production collections")
    cleanup_production_collections(db_client)


@pytest.fixture(scope="module")
def setup_extract_data(db_client: DbConnection):
    log.info("Setting up extract data for tests")

    def load_data(filename: str, collection: ExtractionCollections):
        coll = db_client.get_cfb_collection(Databases.extraction, collection)
        with open(filename, "r", encoding="utf-8") as f:
            data = json.load(f)
            coll.insert_many(data)

    load_data("Tests/data/extract_teams.json", ExtractionCollections.team)
    load_data("Tests/data/extract_conferences.json", ExtractionCollections.conference)
    load_data("Tests/data/extract_venues.json", ExtractionCollections.venue)
    load_data("Tests/data/extract_games.json", ExtractionCollections.game)
    load_data(
        "Tests/data/extract_game_team_stats.json", ExtractionCollections.game_team_stats
    )


@pytest.fixture
def model_checker(subtests):
    def check_model(model: type[CfbBaseModel], entities: list[dict]):
        for entity in entities:
            with subtests.test(entity=entity):
                assert model.model_validate(entity)

    return check_model


@pytest.fixture(scope="module")
def ensure_isolation(request):
    # Check the number of items collected in the current session
    if len(request.session.items) > 1:
        pytest.skip("This test must be run in isolation (by itself).")
