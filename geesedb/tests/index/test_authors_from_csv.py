from os import path

from ...index import AuthorsFromCSV


def test_load_csv_example_files() -> None:
    index = AuthorsFromCSV(database=':memory:',
                           author_doc_file=path.dirname(
                               path.dirname(__file__)) + '/resources/csv/example_author_doc.csv'
                           )

    index.connection.execute("SELECT * FROM author_doc;")
    assert index.connection.fetchone() == ('Mark Giannotto', 'b2e89334-33f9-11e1-825f-dabc29fd7071')
