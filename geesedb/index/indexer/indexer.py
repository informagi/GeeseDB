from ..utils import _create_and_fill_metadata_table, _read_json_file, _get_indexable_line, _alter_docs_table, \
    _update_mem_terms_table, _create_final_tables, _create_main_tables
from ...connection import get_connection
from ..indexer.terms_processor import TermsProcessor
from ..automatic_schema.schema import infer_variable

import json
import os
import pathlib
import time
import sys
from typing import Any
import numpy as np
import pandas as pd


class Indexer:
    """
    Reads, processes and introduces documents of JSONL format in GeeseDB.
    """
    def __init__(self, **kwargs: Any) -> None:
        self.arguments = self.get_arguments(kwargs)
        if self.arguments['use_existing_db'] and os.path.isfile(self.arguments['database']) or \
                not self.arguments['use_existing_db'] and not os.path.isfile(self.arguments['database']):
            pass
        elif not self.arguments['use_existing_db']:
            raise IOError('There already exist a file on this path.')
        db_connection = get_connection(self.arguments['database'])
        self.connection = db_connection.connection
        self.processor = TermsProcessor(self.arguments)
        self.schema = None
        self.init_time = 0

    def run(self) -> None:
        if pathlib.Path(self.arguments['file']).suffix not in ['.jl', '.json']:
            raise IOError('Please provide a valid file')

        t0 = t1 = t2 = t3 = t4 = t5 = 0
        self.init_time = time.time()

        doc_rows, raw_dict_line = _read_json_file(self.connection, self.arguments['file'])
        self.schema = infer_variable(self.connection, json.loads(raw_dict_line).keys(),
                                     self.processor.tokenizer_function) \
            if (self.arguments['infer_schema'] or self.arguments['schema_dict'] is not None) else None
        t0 += time.time() - self.init_time

        if self.schema is None:
            raise Exception('Please provide a valid schema or toggle the automatic schema infer parameter on')
        collection_id_field_name = None
        for k in self.schema.keys():
            if self.schema[k] == '<doc_id>':
                collection_id_field_name = k
                break

        # create all tables
        _create_main_tables(self.connection, collection_id_field_name, doc_rows)
        indexable_column_names = "("
        for k in self.schema.keys():
            if self.schema[k] == '<metadata>':
                _create_and_fill_metadata_table(self.connection, k)
            elif self.schema[k] == '<indexable>':
                indexable_column_names += f"json_object.json->>'{k}', ' ', "
        if indexable_column_names == "(":
            raise IOError("This database does not contain any indexable text or it has not been specified.")
        indexable_column_names = indexable_column_names[:-2] + ")"

        doc_id = 1  # starts at 1 because of how it's created in SQL
        for i in range(int(np.ceil(doc_rows/self.arguments['batch_size']))):
            s = time.time()
            lines_list = _get_indexable_line(self.connection, indexable_column_names, self.arguments['batch_size'], i)
            t1 += time.time() - s
            processed_lines = {'doc_id': [], 'len': [], 'tokens': []}

            for line in lines_list:
                # process line, save in memory
                start = time.time()
                tokens, tokens_str = self.processor.process(line[0])
                tokens_str = tokens_str[:-4]
                tokens_str = tokens_str.replace('%doc_id', str(doc_id))
                t2 += time.time() - start
                processed_lines['tokens'].append(tokens_str)
                processed_lines['doc_id'].append(doc_id)
                processed_lines['len'].append(len(tokens))

                if doc_id % 1000 == 0:
                    sys.stdout.write('\r')
                    sys.stdout.write(f"Processed documents: {doc_id}/{doc_rows} "
                                     f"({np.round(doc_id * 100 / doc_rows, 2)}%) "
                                     f"Overall avg. speed: {np.round(doc_id / (time.time() - self.init_time), 2)} "
                                     f"docs/sec")
                    sys.stdout.flush()
                if doc_id == doc_rows:
                    sys.stdout.write('\r')

                doc_id += 1

            # update docs and term_docs
            df = pd.DataFrame.from_dict(processed_lines)
            start = time.time()
            _alter_docs_table(self.connection, df)
            t3 += time.time() - start
            start = time.time()
            _update_mem_terms_table(self.connection, ', '.join(processed_lines['tokens']))
            t4 += time.time() - start

        start = time.time()
        _create_final_tables(self.connection)
        t5 += time.time() - start

        if self.arguments['print_times']:
            print(f'Running time schema infer:                       {t0} sec')
            print(f'Running time reading line:                       {t1} sec')
            print(f'Running time processing line:                    {t2} sec')
            self.processor.print_times()
            print(f'Running time updating docs:                      {t3} sec')
            print(f'Running time updating dictionary and terms_docs: {t4} sec')
            print(f'Running time filling final tables:               {t5} sec')
            print(f'Running time:                                    {time.time() - self.init_time} sec')

    @staticmethod
    def get_arguments(kwargs: Any) -> dict:
        arguments = {
            'database': None,
            'infer_schema': True,
            'schema_dict': None,
            'use_existing_db': False,
            'create_nltk_data': False,
            'print_times': False,
            'include_html_links': False,
            'tokenization_method': 'syntok',
            'stop_words': 'lucene',
            'stemmer': 'porter',
            'batch_size': 1000,
            'file': 'docs.jl',
            'nltk_path': os.path.dirname(os.path.dirname(os.path.dirname(__file__))) + r'\resources\nltk_data',
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
