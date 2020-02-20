from ...index import FullTextFromCSV
from os import path


def test_load_csv():
    db = FullTextFromCSV(args=['-d',
                               ':memory:',
                               '-di',
                               path.dirname(path.dirname(__file__)) + '/resources/example_files/example_docs.csv',
                               '-ti',
                               path.dirname(path.dirname(__file__)) + '/resources/example_files/example_term_dict.csv',
                               '-oi',
                               path.dirname(path.dirname(__file__)) + '/resources/example_files/example_term_doc.csv'
                               ]
                         )
    db.cursor.execute("SELECT * FROM docs;")
    assert db.cursor.fetchone() == ['document_0', 0, 3]
    assert db.cursor.fetchone() == ['document_1', 1, 4]
