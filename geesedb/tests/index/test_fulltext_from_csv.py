from os import path

from ...index import FullTextFromCSV


def test_load_csv_example_files():
    index = FullTextFromCSV(database=':memory:',
                            docs_file=path.dirname(path.dirname(__file__)) + '/resources/csv/example_docs.csv',
                            term_dict_file=path.dirname(
                                path.dirname(__file__)) + '/resources/csv/example_term_dict.csv',
                            term_doc_file=path.dirname(path.dirname(__file__)) + '/resources/csv/example_term_doc.csv'
                            )
    index.cursor.execute("SELECT * FROM docs;")
    assert index.cursor.fetchone() == ('document_0', 0, 3)
    assert index.cursor.fetchone() == ('document_1', 1, 4)


def test_load_csv_use_existing_database_does_not_exist():
    try:
        FullTextFromCSV(database='test_database',
                        use_existing_db=True
                        )
        assert False
    except IOError:
        assert True
