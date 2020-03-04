from ..generic_text_retrieval_model import GenericTextRetrievalModel
from .aggregate import Aggregate


class BagOfWordsRetrievalModel(GenericTextRetrievalModel, Aggregate):
    def __init__(self):
        GenericTextRetrievalModel.__init__(self)
        Aggregate.__init__(self)

    def construct_query(self, topic):
        super_query = super().construct_query(topic)
        return super_query + ", qterms AS (" \
                             "SELECT term_doc.term_id, term_doc.doc_id, term_doc.tf, qtermids.df " \
                             "FROM term_doc " \
                             "JOIN qtermids " \
                             "ON term_doc.term_id = qtermids.term_id" \
                             ") "

    def get_retrieval_model(self):
        return super().get_retrieval_model()

    def get_aggregator(self):
        return ", scores AS (" \
               "SELECT subscores.collection_id, SUM(subscores.subscore) AS score " \
               "FROM subscores " \
               "GROUP BY subscores.collection_id) "

    def get_create_ranked_list(self):
        return "SELECT scores.collection_id, scores.score " \
               "FROM scores " \
               "ORDER BY scores.score DESC " \
               "LIMIT 1000"
