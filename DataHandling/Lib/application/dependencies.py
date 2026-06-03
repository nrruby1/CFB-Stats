from db.db_connection import DbConnection


def get_db_connection():
    with DbConnection() as db_client:
        yield db_client
