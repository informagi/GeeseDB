class Aggregate:

    def __init__(self) -> None:
        pass

    def get_aggregator(self) -> str:
        raise NotImplementedError("You should implement this method in your retrieval model class.")

    def get_create_ranked_list(self) -> str:
        raise NotImplementedError("You should implement this method in your retrieval model class.")

