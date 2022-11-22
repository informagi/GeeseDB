from ..utils import _create_and_fill_empty_table_with_pd
from ...connection import get_connection
from ..indexer.doc_readers import read_from_WaPo_json
from ..indexer.terms_processor import TermsProcessor

import os
import pandas as pd
import json
import pathlib
import time
import sys
from collections import Counter
from typing import Any


class Indexer:
    """
    Reads, processes and introduces documents of JSONL format in GeeseDB.
    """

    def __init__(self, **kwargs: Any) -> None:
        self.arguments = self.get_arguments(kwargs)
        self.read = read_from_WaPo_json
        self.line = {}
        self.db_docs = {self.arguments['columns_names_docs'][0]: [],
                        self.arguments['columns_names_docs'][1]: [],
                        self.arguments['columns_names_docs'][2]: []}
        self.db_terms = {}
        self.db_terms_docs = {self.arguments['columns_names_term_doc'][0]: [],
                              self.arguments['columns_names_term_doc'][1]: [],
                              self.arguments['columns_names_term_doc'][2]: []}
        db_connection = get_connection(self.arguments['database'])
        self.connection = db_connection.connection
        self.processor = TermsProcessor(self.arguments)
        self.init_time = 0

    def open_and_run(self) -> None:
        self.init_time = time.time()
        if pathlib.Path(self.arguments['file']).suffix == '.jl':
            with open(self.arguments['file'], encoding=self.arguments['encoding']) as file:
                self.run_indexer(file)
        else:
            raise IOError('Please provide a valid file')

    def run_indexer(self, file) -> None:
        t1 = t2 = t3 = t4 = t5 = doc_id = 0

        for line in file:
            self.line = json.loads(line)
            start = time.time()
            self.line = self.read(self.line, doc_id, self.arguments['include_html_links'])
            t1 += time.time() - start
            start = time.time()
            self.line['content'] = self.processor.process(self.line['content'])
            t2 += time.time() - start
            start = time.time()
            self.update_docs_file(doc_id)
            t3 += time.time() - start
            start = time.time()
            self.update_terms_termsdocs_file()
            t4 += time.time() - start
            if doc_id % 100 == 0:
                sys.stdout.write('\r')
                sys.stdout.write("Processed documents: %d" % doc_id)
                sys.stdout.flush()
            doc_id += 1

        start = time.time()
        df_docs = pd.DataFrame.from_dict(self.db_docs)
        new_db_terms = {self.arguments['columns_names_term_dict'][0]: [],
                        self.arguments['columns_names_term_dict'][1]: [],
                        self.arguments['columns_names_term_dict'][2]: []}
        for item in self.db_terms.items():
            new_db_terms['string'].append(item[0])
            new_db_terms['term_id'].append(item[1][0])
            new_db_terms['df'].append(item[1][1])
        df_terms = pd.DataFrame.from_dict(new_db_terms)
        df_terms_docs = pd.DataFrame.from_dict(self.db_terms_docs)

        self.create_and_fill_tables([df_docs, df_terms, df_terms_docs])
        t5 += time.time() - start

        if self.arguments['print_times']:
            print(f'Running time reading line: {t1} sec')
            print(f'Running time processing line: {t2} sec')
            self.processor.print_times()
            print(f'Running time updating docs: {t3} sec')
            print(f'Running time updating dictionary and terms_docs: {t4} sec')
            print(f'Running time creating and filling DB: {t5} sec')
            print(f'Running time: {time.time() - self.init_time} sec')

    @staticmethod
    def get_arguments(kwargs: Any) -> dict:
        arguments = {
            'database': None,
            #'use_existing_db': False,
            #'use_existing_tables': False,
            'create_nltk_data': True,
            'print_times': True,
            'include_html_links': False,
            'tokenization_method': 'syntok',
            'stop_words': 'lucene',
            'stemmer': 'porter',
            'table_names': ['docs', 'term_dict', 'term_doc'],
            'columns_names_docs': ['collection_id', 'doc_id', 'len'],
            'columns_names_term_dict': ['term_id', 'string', 'df'],
            'columns_names_term_doc': ['term_id', 'doc_id', 'tf'],
            'file': 'docs.jl',
            'nltk_path': os.path.dirname(os.path.dirname(os.path.dirname(__file__))) + '\\resources\\nltk_data',
            'language': 'english',
            'encoding': 'utf-8',
            'delimiter': '|',
            'delete_chars': ['', '.', ',', "'", '"', '’', '‘', '“', '”', '-', '—', '(', ')', '[', ']', '{', '}',
                             '<', '>', ':', ';', '?', '!', '/', "\\", '=', '&', '$', '€']
        }
        for key, item in arguments.items():
            if kwargs.get(key) is not None:
                arguments[key] = kwargs.get(key)
        if arguments['database'] is None:
            raise IOError('database path needs to be provided')
        return arguments

    def create_and_fill_tables(self, dataframe_list: []) -> None:
        self.connection.begin()
        for table_name, pd_df in zip(self.arguments['table_names'], dataframe_list):
            _create_and_fill_empty_table_with_pd(self.connection, table_name, pd_df)
        self.connection.commit()

    def update_docs_file(self, doc_id: int) -> None:  # input a line, update csv
        self.db_docs['collection_id'].append(self.line['collection_id'])
        self.db_docs['doc_id'].append(doc_id)
        self.db_docs['len'].append(len(self.line['content']))

    def update_terms_termsdocs_file(self) -> None:
        temp_words = Counter(self.line['content']).keys()  # unique elements
        temp_vals = Counter(self.line['content']).values()  # counts the elements' frequency
        for w, t in zip(temp_words, temp_vals):
            if w not in self.db_terms.keys():
                i = len(self.db_terms)
                self.db_terms[w] = [i, 1]  # term_id, df
                self.db_terms_docs['term_id'].append(i)
            else:  # find word and update df (once per doc)
                self.db_terms[w][1] += 1
                self.db_terms_docs['term_id'].append(self.db_terms[w][0])  # find term id of word w
            self.db_terms_docs['doc_id'].append(self.line['doc_id'])
            self.db_terms_docs['tf'].append(t)
