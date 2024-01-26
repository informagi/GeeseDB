import argparse
from pathlib import Path
from typing import Any

from tqdm import tqdm
from ciff_toolkit.ciff_pb2 import Header, Posting, PostingsList, DocRecord
from ciff_toolkit.write import CiffWriter

from ...connection import get_connection


class ToCiff:

    def __init__(self, **kwargs: Any) -> None:
        self.arguments = self.get_arguments(kwargs)
        db_connection = get_connection(self.arguments['database'])
        self.connection = db_connection.connection
        self.cursor = db_connection.cursor
        self.create_ciff()

    @staticmethod
    def get_arguments(kwargs: Any) -> dict:
        arguments = {
            'database': None,
            'ciff': None,
            'docs': 'docs',
            'term_dict': 'term_dict',
            'term_doc': 'term_doc',
            'batch_size': 1000,
            'verbose': False,
        }
        for key, item in arguments.items():
            if kwargs.get(key) is not None:
                arguments[key] = kwargs.get(key)
        return arguments

    def create_ciff(self) -> None:
        disable_tqdm = not self.arguments['verbose']

        with CiffWriter(self.arguments['ciff']) as writer:
            header = self.get_ciff_header()
            writer.write_header(header)

            postings_lists = tqdm(self.get_ciff_postings_lists(), total=header.num_postings_lists, disable=disable_tqdm)
            writer.write_postings_lists(postings_lists)

            doc_records = tqdm(self.get_ciff_doc_records(), total=header.num_docs, disable=disable_tqdm)
            writer.write_documents(doc_records)

    def get_ciff_header(self):
        header = Header()
        header.version = 1  # We work with ciff v1
        self.cursor.execute("""SELECT COUNT(*) FROM term_dict""")
        header.num_postings_lists = self.cursor.fetchone()[0]
        self.cursor.execute("""SELECT COUNT(*) FROM docs""")
        header.num_docs = self.cursor.fetchone()[0]
        header.total_postings_lists = header.num_postings_lists
        header.total_docs = header.num_docs
        self.cursor.execute("""SELECT SUM(tf) FROM term_doc""")
        header.total_terms_in_collection = self.cursor.fetchone()[0]
        header.average_doclength = header.total_terms_in_collection / header.num_docs
        header.description = f'GeeseDB database {self.arguments["database"]}'

        return header

    def get_ciff_postings_lists(self):
        self.cursor.execute("""
            SELECT df, string, list(row(doc_id, tf) ORDER BY doc_id) 
            FROM term_dict, term_doc 
            WHERE term_dict.term_id = term_doc.term_id 
            GROUP BY term_dict.term_id, df, string 
            ORDER BY string 
        """)
        while batch := self.cursor.fetchmany(self.arguments['batch_size']):
            for df, term, postings in batch:
                postings_list = PostingsList()
                assert len(postings) == df
                cf = sum(p['tf'] for p in postings)
                postings_list.term = term
                postings_list.df = df
                postings_list.cf = cf
                old_id = 0
                for p in postings:
                    posting = Posting()
                    doc_id = p['doc_id']
                    tf = p['tf']
                    posting.docid = doc_id - old_id
                    old_id = doc_id
                    posting.tf = tf
                    postings_list.postings.append(posting)

                yield postings_list

    def get_ciff_doc_records(self):
        self.cursor.execute("""
            SELECT doc_id, collection_id, len
            FROM docs
            ORDER BY doc_id
        """)
        while batch := self.cursor.fetchmany(self.arguments['batch_size']):
            for doc_id, collection_id, length in batch:
                doc_record = DocRecord()
                doc_record.docid = doc_id
                doc_record.collection_docid = collection_id
                doc_record.doclength = length

                yield doc_record


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('--database', required=True)
    parser.add_argument('--ciff', required=True)
    parser.add_argument('--docs')
    parser.add_argument('--term_dict')
    parser.add_argument('--term_doc')
    parser.add_argument('--batch_size', type=int)
    parser.add_argument('--verbose', action='store_true')
    args = parser.parse_args()
    ToCiff(**vars(args))
