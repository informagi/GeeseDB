#! /usr/bin/env python3

import argparse
import gzip
import os
from typing import Any, List, Union, Tuple

from ..connection import DBConnection
from ..utils import CommonIndexFileFormat_pb2 as Ciff


class FullTextFromCiff:
    """
    Class for creating tables as in the old dog paper:
    - https://dl.acm.org/doi/10.1145/2600428.2609460

    The tables are created from a CIFF as described in:
    - https://arxiv.org/abs/2003.08276
    """
    _COLUMN_TYPES = [
        ['STRING', 'INT', 'INT'],
        ['INT', 'INT', 'STRING'],
        ['INT', 'INT', 'INT']
    ]

    def __init__(self, **kwargs: Any) -> None:
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
            'protobuf_file': None
        }
        for key, item in arguments.items():
            if kwargs.get(key) is not None:
                arguments[key] = kwargs.get(key)
        if arguments['database'] is None:
            raise IOError('database path needs to be provided')
        if arguments['protobuf_file'] is None:
            raise IOError('protobuf file needs to be provided')
        return arguments

    def create_tables(self) -> None:
        column_names = [
            self.arguments['columns_names_docs'],
            self.arguments['columns_names_term_dict'],
            self.arguments['columns_names_term_doc']
        ]
        self.connection.begin()
        for table_name, c_names, c_types in zip(self.arguments['table_names'], column_names, self._COLUMN_TYPES):
            self.create_table(table_name, c_names, c_types)
        self.connection.commit()

    def create_table(self, table_name: str, column_names: List[str], column_types: List[str]) -> None:
        try:
            self.cursor.execute(f'SELECT * FROM {table_name} LIMIT 1;')
            self.connection.rollback()
            raise IOError('Table already exists.')
        except RuntimeError:  # If the table does not exists you get a RuntimeError
            pass
        query = f'CREATE TABLE {table_name} ({", ".join([f"{a} {b}" for a, b in zip(column_names, column_types)])});'
        self.cursor.execute(query)

    @staticmethod
    def decode(buffer: Union[str, bytes], pos: int) -> Union[Tuple[int, int], None]:
        mask = (1 << 32) - 1
        result = 0
        shift = 0
        while True:
            b = buffer[pos]
            result |= ((b & 0x7f) << shift)
            pos += 1
            if not (b & 0x80):
                result &= mask
                result = int(result)
                return result, pos
            shift += 7
            if shift >= 64:
                raise IOError('Too many bytes when decoding.')

    def fill_tables(self) -> None:
        if self.arguments['protobuf_file'].endswith('.gz'):
            with gzip.open(self.arguments['protobuf_file'], 'rb') as f:
                data = f.read()
        else:
            with open(self.arguments['protobuf_file'], 'rb') as f:
                data = f.read()

        # start with reading header info
        next_pos, pos = 0, 0
        header = Ciff.Header()
        next_pos, pos = self.decode(data, pos)
        header.ParseFromString(data[pos:pos+next_pos])
        pos += next_pos

        # read posting lists
        postings_list = Ciff.PostingsList()
        for term_id in range(header.num_postings_lists):
            self.connection.begin()
            next_pos, pos = self.decode(data, pos)
            postings_list.ParseFromString(data[pos:pos+next_pos])
            pos += next_pos
            q = f'INSERT INTO {self.arguments["table_names"][1]} ' \
                f'({",".join(self.arguments["columns_names_term_dict"])}) ' \
                f"VALUES ({term_id},{postings_list.df},'{postings_list.term}')"
            try:
                self.cursor.execute(q)
            except RuntimeError:
                print(q)
            for posting in postings_list.postings:
                q = f'INSERT INTO {self.arguments["table_names"][2]} ' \
                    f'({",".join(self.arguments["columns_names_term_doc"])}) ' \
                    f'VALUES ({term_id},{posting.docid},{posting.tf})'
                self.cursor.execute(q)
            self.connection.commit()

        # read doc information
        doc_record = Ciff.DocRecord()
        self.connection.begin()
        for n in range(header.num_docs):
            if n % 1000 == 0:
                self.connection.commit()
                self.connection.begin()
            next_pos, pos = self.decode(data, pos)
            doc_record.ParseFromString(data[pos:pos+next_pos])
            pos += next_pos
            q = f'INSERT INTO {self.arguments["table_names"][0]} ' \
                f'({",".join(self.arguments["columns_names_docs"])}) ' \
                f"VALUES ('{doc_record.collection_docid}',{doc_record.docid},{doc_record.doclength})"
            self.cursor.execute(q)
        self.connection.commit()


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('-d',
                        '--database',
                        required=True,
                        metavar='[file]',
                        help='Location of the database.')
    parser.add_argument('-p',
                        '--protobuf_file',
                        required=True,
                        metavar='[file]',
                        help='Filename for the csv file containing the data for the docs table.')
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
    FullTextFromCiff(**vars(parser.parse_args()))
