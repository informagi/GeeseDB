#! /usr/bin/env python3.6

import argparse
import os
from typing import Any, List

from .utils import _fill_empty_table_with_csv, _create_table
from ..connection import DBConnection


class FullTextFromCSV:
    """
    Class for creating tables from csv files as in the old dog paper:
    - https://dl.acm.org/doi/10.1145/2600428.2609460
    """
    _COLUMN_TYPES = [
        ['STRING', 'INT', 'INT'],
        ['INT', 'INT', 'STRING'],
        ['INT', 'INT', 'INT']
    ]

    def __init__(self, **kwargs: List[Any]) -> None:
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

        if not self.arguments['use_existing_db']:
            self.create_tables()
        self.fill_tables()

    @staticmethod
    def get_arguments(kwargs: Any) -> dict:
        arguments = {
            'database': None,
            'use_existing_db': False,
            'use_existing_tables': False,
            'table_names': ['docs', 'term_dict', 'term_doc'],
            'columns_names_docs': ['collection_id', 'doc_id', 'len'],
            'columns_names_term_dict': ['term_id', 'df', 'string'],
            'columns_names_term_doc': ['term_id', 'doc_id', 'tf'],
            'docs_file': 'docs.csv',
            'term_dict_file': 'dict.csv',
            'term_doc_file': 'term_doc.csv',
            'delimiter': '|'
        }
        for key, item in arguments.items():
            if kwargs.get(key) is not None:
                arguments[key] = kwargs.get(key)
        if arguments['database'] is None:
            raise IOError('database path needs to be provided')
        return arguments

    def create_tables(self) -> None:
        column_names = [
            self.arguments['columns_names_docs'],
            self.arguments['columns_names_term_dict'],
            self.arguments['columns_names_term_doc']
        ]
        self.connection.begin()
        for table_name, c_names, c_types in zip(self.arguments['table_names'], column_names, self._COLUMN_TYPES):
            _create_table(self.connection, table_name, c_names, c_types)
        self.connection.commit()

    def fill_tables(self) -> None:
        file_names = [
            self.arguments['docs_file'],
            self.arguments['term_dict_file'],
            self.arguments['term_doc_file']
        ]
        self.connection.begin()
        for table_name, file_name in zip(self.arguments['table_names'], file_names):
            _fill_empty_table_with_csv(self.connection, table_name, file_name, self.arguments['delimiter'])
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
    parser.add_argument('-t',
                        '--table_names',
                        metavar='[string]',
                        nargs=3,
                        help='Decide on the table names you want to fill if they exist, ' +
                             'or create and fill them if they do not exist. If no names ' +
                             'are given the default values ["docs.csv", "term_dict.csv", ' +
                             '"term_doc.csv"] are being used. If arguments are given ' +
                             'they are expected in the respective default order.')
    parser.add_argument('-cd',
                        '--columns_names_docs',
                        metavar='[string]',
                        nargs=2,
                        help='Column names for the docs table.')
    parser.add_argument('-ct',
                        '--columns_names_term_dict',
                        metavar='[string]',
                        nargs=3,
                        help='Column names for the dict table.')
    parser.add_argument('-o',
                        '--columns_names_term_doc',
                        metavar='[string]',
                        nargs=3,
                        help='Column names for the term-docs table (docs in old dog paper).')
    parser.add_argument('-di',
                        '--docs_file',
                        metavar='[file]',
                        help='Filename for the csv file containing the data for the docs table.')
    parser.add_argument('-ti',
                        '--term_dict_file',
                        metavar='[file]',
                        help='Filename for the csv file containing the data for the dict table.')
    parser.add_argument('-oi',
                        '--term_doc_file',
                        metavar='[file]',
                        help='Filename for the csv file containing the data for the term-docs table ' +
                             '(terms in old dog paper).')
    parser.add_argument('-e',
                        '--delimiter',
                        help='Delimiter that separates the columns in the csv files.')
    FullTextFromCSV(**vars(parser.parse_args()))
