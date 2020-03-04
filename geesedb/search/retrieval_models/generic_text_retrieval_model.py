class GenericTextRetrievalModel:

    def __init__(self):
        pass

    def construct_query(self, topic):
        return "WITH qtermids AS (" \
               "SELECT term_dict.term_id, term_dict.df " \
               "FROM term_dict " \
               "WHERE term_dict.string IN ('{}')" \
               ")".format("', '".join(topic.split(' ')))

    def get_retrieval_model(self):
        raise NotImplementedError("You should implement this method in your retrieval model class.")

