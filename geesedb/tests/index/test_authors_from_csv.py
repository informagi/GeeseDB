from os import path

from ...index import AuthorsFromCSV


def test_load_csv_example_files():
    index = AuthorsFromCSV(database=':memory:',
                           doc_author_file=path.dirname(
                               path.dirname(__file__)) + '/resources/csv/example_docs_authors.csv'
                           )

    index.cursor.execute("SELECT * FROM doc_author;")
    assert index.cursor.fetchone() == ['b2e89334-33f9-11e1-825f-dabc29fd7071', 'Mark Giannotto']
