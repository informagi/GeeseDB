import json

from ..connection import get_connection

class Metadata:

    def __init__(self, database):
        self.connection = get_connection(database)

    # first list is default if nothing is specified (should be extended)
    # list is ordered as [edge_name, node1_id, edge_node1_id, edge_node2_id, node2_id2
    def get_metadata(self):
        self.connection.execute("SELECT metadata FROM _meta")
        metadata = json.loads(self.connection.fetchone()[0])
        return metadata

    def update_metadata(self, data):
        self.connection.execute(f"UPDATE _meta SET metadata='{json.dumps(data)}'")

    def get_default_join_info(self, node1, node2):
        return self.get_metadata()[node1][node2][0]

    # {
    #     'term_dict': {
    #         'docs': [['term_doc', 'term_id', 'term_id', 'doc_id', 'doc_id']]
    #     },
    #     'docs': {
    #         'term_dict': [['term_doc', 'doc_id', 'doc_id', 'term_id', 'term_id']],
    #         'entities': [['entity_doc', 'collection_id', 'doc_id', 'entity', 'entity']],
    #         'authors': [['doc_author', 'collection_id', 'doc', 'author', 'author']]
    #     },
    #     'entities': {
    #         'docs': [['entity_doc', 'entity', 'entity', 'doc_id', 'collection_id']]
    #     },
    #     'authors': {
    #         'docs': [['doc_author', 'author', 'author', 'doc', 'collection_id']]
    #     }
    # }