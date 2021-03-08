import argparse
import os

from ..connection import DBConnection
from .utils import _create_table, _fill_empty_table_with_csv


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

        if not self.arguments['use_existing_tables']:
            _create_table(self.connection, self.arguments['table_name'], self.arguments['columns_names'],
                          self._COLUMN_TYPES)
        _fill_empty_table_with_csv(self. connection, self.arguments['table_name'], self.arguments['author_doc_file'],
                                   self.arguments['delimiter'])

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
