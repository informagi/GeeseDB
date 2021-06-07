class Metadata:

    def __init__(self):
        # first list is default if nothing is specified (should be extended)
        # list is ordered as [edge_name, node1_id, edge_node1_id, edge_node2_id, node2_id2
        self.metadata = {
            'term_dict': {
                'docs': [['term_doc', 'term_id', 'term_id', 'doc_id', 'doc_id']]
            },
            'docs': {
                'term_dict': [['term_doc', 'doc_id', 'doc_id', 'term_id', 'term_id']],
                'entities': [['entity_doc', 'collection_id', 'doc_id', 'entity', 'entity']],
                'authors': [['doc_author', 'collection_id', 'doc', 'author', 'author']]
            },
            'entities': {
                'docs': [['entity_doc', 'entity', 'entity', 'doc_id', 'collection_id']]
            },
            'authors': {
                'docs': [['doc_author', 'author', 'author', 'doc', 'collection_id']]
            }
        }

    def get_default_join_info(self, node1, node2):
        return self.metadata[node1][node2][0]