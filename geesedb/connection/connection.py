import duckdb


class DBConnection:

    def __init__(self, database: str) -> None:
        self.connection = duckdb.connect(database)
        self.cursor = self.connection.cursor()
