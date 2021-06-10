class GenericTextRetrievalModel:

    def __init__(self) -> None:
        pass

    def construct_query(self, topic: str) -> str:
        return "WITH qtermids AS (" \
               "SELECT term_dict.term_id, term_dict.df " \
               "FROM term_dict " \
               "WHERE term_dict.string IN ('{}')" \
               ")".format("', '".join(topic.split(' ')))

    def get_retrieval_model(self) -> str:
        raise NotImplementedError("You should implement this method in your retrieval model class.")
