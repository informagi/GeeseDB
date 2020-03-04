from ..bow_retrieval_model import BagOfWordsRetrievalModel


class DisjunctiveRetrievalModel(BagOfWordsRetrievalModel):

    def __init__(self):
        super().__init__()

    def get_aggregator(self):
        return super().get_aggregator()

    def construct_query(self, topic):
        return super().construct_query(topic) + \
               ", condocs AS (" \
               "SELECT qterms.doc_id " \
               "FROM qterms " \
               "GROUP BY qterms.doc_id)"

    def get_create_ranked_list(self):
        return super().get_create_ranked_list()

    def get_retrieval_model(self):
        return super().get_retrieval_model()
