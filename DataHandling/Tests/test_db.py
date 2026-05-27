import pytest

from db.db_connection import *
from db.model.team import Team


def test_get_databases(db_client: DbConnection):
    db = db_client.get_cfb_database(Databases.extraction)
    assert db != None


def test_get_collections(db_client: DbConnection):
    db = db_client.get_cfb_collection(Databases.extraction, ExtractionCollections.team)
    assert db != None

    db = db_client.get_cfb_collection(Databases.staging, Team)
    assert db != None

    db = db_client.get_cfb_collection(Databases.production, Team)
    assert db != None


def test_get_collections_invalid(db_client: DbConnection):
    with pytest.raises(Exception):
        db = db_client.get_cfb_collection(Databases.staging, ExtractionCollections.team)

    with pytest.raises(Exception):
        db = db_client.get_cfb_collection(Databases.extraction, Team)


def test_get_repositories(db_client: DbConnection):
    db = db_client.get_cfb_repository(Databases.staging, Team)
    assert db != None

    db = db_client.get_cfb_repository(Databases.production, Team)
    assert db != None


def test_get_repositories_invalid(db_client: DbConnection):
    with pytest.raises(Exception):
        db = db_client.get_cfb_repository(Databases.extraction, Team)


def test_get_collection_namespace(db_client: DbConnection):
    ns = db_client.get_collection_namespace(
        Databases.extraction, ExtractionCollections.team
    )
    assert ns == "test_cfb_extraction.team"

    ns = db_client.get_collection_namespace(Databases.staging, Team)
    assert ns == "test_cfb_staging.team"

    ns = db_client.get_collection_namespace(Databases.production, Team)
    assert ns == "test_cfb_data.team"


def test_get_collection_namespace_invalid(db_client: DbConnection):
    with pytest.raises(Exception):
        ns = db_client.get_collection_namespace(
            Databases.staging, ExtractionCollections.team
        )

    with pytest.raises(Exception):
        ns = db_client.get_collection_namespace(Databases.extraction, Team)
