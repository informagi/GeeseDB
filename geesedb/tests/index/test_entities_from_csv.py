from os import path

from ...index import EntitiesFromCSV


def test_load_csv_example_files():
    index = EntitiesFromCSV(database=':memory:',
                            entity_doc_file=path.dirname(
                                path.dirname(__file__)) + '/resources/csv/example_entity_doc.csv'
                            )

    index.cursor.execute("SELECT * FROM entity_doc;")
    first = index.cursor.fetchone()
    assert first[0] == 0
    assert first[1] == 11
    assert first[2] == 'Danny Coale'
    assert first[3] == 'Danny_Coale'
    assert abs(first[4] - 0.38727825954758405) < 0.001
    assert abs(first[5] - 0.9888599514961243) < 0.001
    assert first[6] == 'PER'
    assert first[7] == 'b2e89334-33f9-11e1-825f-dabc29fd7071'

