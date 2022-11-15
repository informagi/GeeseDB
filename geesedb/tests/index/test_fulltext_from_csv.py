from os import path

from geesedb.index import FullTextFromCSV
from geesedb.connection import close_connection


def test_load_csv_example_files() -> None:
    index = FullTextFromCSV(database=':memory:',
                            docs_file=path.dirname(path.dirname(__file__)) + '\\resources\\csv\\example_docs.csv',
                            term_dict_file=path.dirname(
                                path.dirname(__file__)) + '\\resources\\csv\\example_term_dict.csv',
                            term_doc_file=path.dirname(path.dirname(__file__)) + '\\resources\\csv\\example_term_doc.csv'
                            )
    index.load_data()
    index.connection.execute("SELECT * FROM docs;")
    assert index.connection.fetchone() == ('document_0', 0, 3)
    assert index.connection.fetchone() == ('document_1', 1, 4)
    close_connection()


def test_load_csv_use_existing_database_does_not_exist() -> None:
    try:
        FullTextFromCSV(database='test_database',
                        use_existing_db=True
                        )
        assert False
    except IOError:
        assert True
    close_connection()
