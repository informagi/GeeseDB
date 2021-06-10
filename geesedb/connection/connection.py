import duckdb

_db_connection = None


def get_connection(database):
    global _db_connection
    if not _db_connection:
        _db_connection = DBConnection(database)
    return _db_connection


def close_connection():
    global _db_connection
    if _db_connection:
        _db_connection.connection.close()
    _db_connection = None


class DBConnection(object):

    def __init__(self, database: str) -> None:
        self.connection = duckdb.connect(database)
        self.cursor = self.connection.cursor()
