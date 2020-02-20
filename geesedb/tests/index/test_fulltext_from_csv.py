from os import path

from ...index import FullTextFromCSV


def test_load_csv_example_files():
    index = FullTextFromCSV(
        args=[
            '-d', ':memory:',
            '-di', path.dirname(path.dirname(__file__)) + '/resources/example_files/example_docs.csv',
            '-ti', path.dirname(path.dirname(__file__)) + '/resources/example_files/example_term_dict.csv',
            '-oi', path.dirname(path.dirname(__file__)) + '/resources/example_files/example_term_doc.csv'
        ]
    )
    index.cursor.execute("SELECT * FROM docs;")
    assert index.cursor.fetchone() == ['document_0', 0, 3]
    assert index.cursor.fetchone() == ['document_1', 1, 4]


def test_load_csv_use_existing_database_does_not_exist():
    try:
        FullTextFromCSV(
            args=[
                '-d', 'test_database',
                '-u'
            ]
        )
        assert False
    except IOError:
        assert True
