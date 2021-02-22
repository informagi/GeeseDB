from os import path

from ...index import FullTextFromCiff


def test_load_csv_example_files():
    index = FullTextFromCiff(database=':memory:',
                             protobuf_file=path.dirname(path.dirname(__file__)
                                                        ) + '/resources/ciff/toy-complete-20200309.ciff.gz'
                             )
    index.cursor.execute("SELECT * FROM docs;")
    assert index.cursor.fetchone() == ('WSJ_1', 0, 6)
    assert index.cursor.fetchone() == ('TREC_DOC_1', 1, 4)
