from os import path

from ...index import FullTextFromCiff
from ...connection import close_connection

def test_load_csv_example_files() -> None:
    index = FullTextFromCiff(database=':memory:',
                             protobuf_file=path.dirname(path.dirname(__file__)
                                                        ) + '/resources/ciff/toy-complete-20200309.ciff.gz'
                             )
    index.load_data()
    index.cursor.execute("SELECT * FROM docs;")
    assert index.cursor.fetchone() == ('WSJ_1', 0, 6)
    assert index.cursor.fetchone() == ('TREC_DOC_1', 1, 4)
    close_connection()
