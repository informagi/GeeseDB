import argparse
import os

from ..connection import DBConnection


class AuthorsFromCSV:
    """
    Class for creating table from csv file that contains author information
    Author - doc
    """
    _COLUMN_TYPES = ['STRING', 'STRING']

    def __init__(self, **kwargs):
        self.arguments = self.get_arguments(kwargs)
        if self.arguments['use_existing_db'] and os.path.isfile(self.arguments['database']) or \
                not self.arguments['use_existing_db'] and not os.path.isfile(self.arguments['database']):
            pass
        elif not self.arguments['use_existing_db']:
            raise IOError('There already exist a file on this path.')
        else:
            raise IOError('Database does not exist.')
        db_connection = DBConnection(self.arguments['database'])
        self.connection = db_connection.connection
        self.cursor = db_connection.cursor

        if not self.arguments['use_existing_tables']:
            self.create_table(self.arguments['table_name'], self.arguments['columns_names'], self._COLUMN_TYPES)
        self.fill_table(self.arguments['table_name'], self.arguments['author_doc_file'], self.arguments['delimiter'])

    @staticmethod
    def get_arguments(kwargs):
        arguments = {
            'database': None,
            'use_existing_db': False,
            'use_existing_tables': False,
            'author_doc_file': 'author_doc.csv',
            'table_name': 'author_doc',
            'columns_names': ['author', 'doc'],
            'delimiter': '|'
        }
        for key, item in arguments.items():
            if kwargs.get(key) is not None:
                arguments[key] = kwargs.get(key)
        if arguments['database'] is None:
            raise IOError('database path needs to be provided')
        return arguments

    def create_table(self, table_name, column_names, column_types):
        self.connection.begin()
        try:
            self.cursor.execute(f'SELECT * FROM {table_name} LIMIT 1;')
            self.connection.rollback()
            raise IOError('Table already exists.')
        except RuntimeError:  # If the table does not exists you get a RuntimeError
            pass
        query = f'CREATE TABLE {table_name} ({", ".join([f"{a} {b}" for a, b in zip(column_names, column_types)])});'
        self.cursor.execute(query)
        self.connection.commit()

    def fill_table(self, table_name, file_name, delimiter):
        self.connection.begin()
        self.cursor.execute(f'SELECT COUNT(*) FROM {table_name};')
        if self.cursor.fetchone()[0] > 0:
            self.connection.rollback()
            raise IOError('The tables are not empty.')
        query = f"COPY {table_name} FROM '{file_name}' WITH DELIMITER '{delimiter}';"
        self.cursor.execute(query)
        self.connection.commit()


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('-d',
                        '--database',
                        required=True,
                        metavar='[file]',
                        help='Location of the database.')
    parser.add_argument('-u',
                        '--use_existing_db',
                        action='store_true',
                        help='Use an existing database.')
    parser.add_argument('-s',
                        '--use_existing_tables',
                        action='store_true',
                        help='Use existing tables.')
    parser.add_argument('-a',
                        '--author_doc_file',
                        metavar='[file]',
                        help='Filename for the csv file containing the data for the docs table.')
    parser.add_argument('-t',
                        '--table_name',
                        metavar='[string]',
                        help='Decide on the table name you want to fill if they exist, ' +
                             'or create and fill them if they do not exist. If no name ' +
                             'is given the default value "author_doc" are being used.')
    parser.add_argument('-c',
                        '--columns_names',
                        metavar='[string]',
                        nargs=2,
                        help='Column names for the author-doc table.')
    parser.add_argument('-e',
                        '--delimiter',
                        help='Delimiter that separates the columns in the csv files.')
    AuthorsFromCSV(**vars(parser.parse_args()))
