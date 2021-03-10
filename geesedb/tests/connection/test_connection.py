from ...connection import DBConnection


def test_create_connection() -> None:
    db_connection = DBConnection(':memory:')
    cursor = db_connection.cursor
    cursor.execute("SELECT 1;")
    assert cursor.fetchone() == (1,)
