from ...connection import get_connection, close_connection


def test_create_connection() -> None:
    db_connection = get_connection(':memory:')
    cursor = db_connection.cursor
    cursor.execute("SELECT 1;")
    assert cursor.fetchone() == (1,)
    close_connection()
