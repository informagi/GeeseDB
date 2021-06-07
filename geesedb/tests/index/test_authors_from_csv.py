from os import path

from ...index import AuthorsFromCSV
from ...connection import close_connection


def test_load_csv_example_files() -> None:
    index = AuthorsFromCSV(database=':memory:',
                           doc_author_file=path.dirname(
                               path.dirname(__file__)) + '/resources/csv/example_doc_author.csv'
                           )

    index.connection.execute("SELECT * FROM doc_author;")
    assert index.connection.fetchone() == ('b2e89334-33f9-11e1-825f-dabc29fd7071', 'Mark Giannotto')
    close_connection()
