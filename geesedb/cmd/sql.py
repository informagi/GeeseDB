import cmd, argparse
from typing import Any
from ..connection import DBConnection

class SQL(cmd.Cmd):
    intro = 'SQL shell powered by DuckDB. Type help or ? to list commands.\n'
    prompt = '(sql) '

    def __init__(self, **kwargs: Any) -> None:
        self.arguments = self.get_arguments(kwargs)
        self.db_connection = DBConnection(self.arguments['database'])
        self.cursor = self.db_connection.cursor
        super(SQL, self).__init__()

    @staticmethod
    def get_arguments(kwargs: Any) -> dict:
        arguments = {
            'database': None
        }
        for key, item in arguments.items():
            if kwargs.get(key) is not None:
                arguments[key] = kwargs.get(key)
        if arguments['database'] is None:
            raise IOError('database path needs to be provided')
        return arguments

    def do_quit(self, arg) -> bool:
        """Exit this shell"""
        return True

    def do_fetchall(self, arg) -> None:
        """Fetch all results after issuing a SQL query"""
        print(self.cursor.fetchall())

    def do_fetchone(self, arg) -> None:
        """Fetch a row after issuing a SQL query"""
        print(self.cursor.fetchone())

    def default(self, line: str) -> None:
        """Issue a sql query"""
        try:
            self.cursor.execute(line)
        except RuntimeError as error:
            print(error)

if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('-d',
                        '--database',
                        required=True,
                        metavar='[file]',
                        help='Location of the database.')
    SQL(**vars(parser.parse_args())).cmdloop()