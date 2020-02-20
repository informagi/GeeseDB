#! /usr/bin/env python3.6

import argparse
import duckdb
import os
import sys

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

    def __init__(self, args=sys.argv[1:]):
        parser = self.parse_args()
        self.arguments = parser.parse_args(args)
        if self.arguments.use_existing_db and os.path.isfile(self.arguments.database) or \
                not self.arguments.use_existing_db and not os.path.isfile(self.arguments.database):
            pass
        elif not self.arguments.use_existing_db:
            raise IOError('There already exist a file on this path.')
        else:
            raise IOError('Database does not exist.')
        db_connection = DBConnection(self.arguments.database)
        self.connection = db_connection.connection
        self.cursor = db_connection.cursor

        if not self.arguments.use_existing_tables:
            self.create_tables()
        self.fill_tables()

    def create_tables(self):
        column_names = [
            self.arguments.columns_names_docs,
            self.arguments.columns_names_term_dict,
            self.arguments.columns_names_term_doc
        ]
        self.connection.begin()
        for table_name, c_names, c_types in zip(self.arguments.table_names, column_names, self._COLUMN_TYPES):
            self.create_table(table_name, c_names, c_types)
        self.connection.commit()

    def create_table(self, table_name, column_names, column_types):
        try:
            self.cursor.execute(f'SELECT * FROM {table_name} LIMIT 1;')
            self.connection.rollback()
            raise IOError('Table already exists.')
        except duckdb.DatabaseError:
            pass
        query = f'CREATE TABLE {table_name} ({", ".join([f"{a} {b}" for a, b in zip(column_names, column_types)])});'
        self.cursor.execute(query)

    def fill_tables(self):
        file_names = [
            self.arguments.docs_file,
            self.arguments.term_dict_file,
            self.arguments.term_doc_file
        ]
        self.connection.begin()
        for table_name, file_name in zip(self.arguments.table_names, file_names):
            self.fill_table(table_name, file_name, self.arguments.delimiter)
        self.connection.commit()

    def fill_table(self, table_name, file_name, delimiter="|"):
        self.cursor.execute(f'SELECT COUNT(*) FROM {table_name};')
        if self.cursor.fetchone()[0] > 0:
            self.connection.rollback()
            raise IOError('The tables are not empty.')
        query = f"COPY {table_name} FROM '{file_name}' WITH DELIMITER '{delimiter}';"
        self.cursor.execute(query)

    @staticmethod
    def parse_args():
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
                                 'they are expected in the respective default order.',
                            default=['docs', 'term_dict', 'term_doc'])
        parser.add_argument('-cd',
                            '--columns_names_docs',
                            metavar='[string]',
                            nargs=2,
                            default=['doc_id', 'collection_id', 'len'],
                            help='Column names for the docs table.')
        parser.add_argument('-ct',
                            '--columns_names_term_dict',
                            metavar='[string]',
                            nargs=3,
                            default=['term_id', 'df', 'string'],
                            help='Column names for the dict table.')
        parser.add_argument('-o',
                            '--columns_names_term_doc',
                            metavar='[string]',
                            nargs=3,
                            default=['term_id', 'doc_id', 'tf'],
                            help='Column names for the term-docs table (docs in old dog paper).')
        parser.add_argument('-di',
                            '--docs_file',
                            default='docs.csv',
                            metavar='[file]',
                            help='Filename for the csv file containing the data for the docs table.')
        parser.add_argument('-ti',
                            '--term_dict_file',
                            default='dict.csv',
                            metavar='[file]',
                            help='Filename for the csv file containing the data for the dict table.')
        parser.add_argument('-oi',
                            '--term_doc_file',
                            default='term_docs.csv',
                            metavar='[file]',
                            help='Filename for the csv file containing the data for the term-docs table ' +
                                 '(docs in old dog paper).')
        parser.add_argument('-e',
                            '--delimiter',
                            default='|',
                            help='Delimiter that separates the columns in the csv files.')
        return parser


if __name__ == '__main__':
    FullTextFromCSV()
