from os import path

from ...index import EntitiesFromCSV


def test_load_csv_example_files():
    index = EntitiesFromCSV(database=':memory:',
                            entity_doc_file=path.dirname(
                                path.dirname(__file__)) + '/resources/csv/example_entity_doc.csv'
                            )

    index.connection.execute("SELECT * FROM entity_doc;")
    assert index.connection.fetchone() == (0, 11, 'Danny Coale', 'Danny_Coale', 'PER',
                                           'b2e89334-33f9-11e1-825f-dabc29fd7071')

