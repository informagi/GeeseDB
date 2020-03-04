class Aggregate:

    def __init__(self):
        pass

    def get_aggregator(self):
        raise NotImplementedError("You should implement this method in your retrieval model class.")

    def get_create_ranked_list(self):
        raise NotImplementedError("You should implement this method in your retrieval model class.")

