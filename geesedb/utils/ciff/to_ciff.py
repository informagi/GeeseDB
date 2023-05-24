import argparse
from pathlib import Path
from typing import Any

from .CommonIndexFileFormat_pb2 import Header, Posting, PostingsList, DocRecord
from ...connection import get_connection
from .ciff_writer import MessageWriter


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
            'delimiter': '|'
        }
        for key, item in arguments.items():
            if kwargs.get(key) is not None:
                arguments[key] = kwargs.get(key)
        return arguments

    def create_ciff(self) -> None:
        with MessageWriter(Path(self.arguments['ciff'])) as f:
            # Create header
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
            header.description = 'This is the first experimental output of (part of) the CommonCrawl in CIFF'
            f.write_message(header)

            # Create postings lists
            self.cursor.execute("""
                SELECT df, string, list(row(doc_id, tf)) 
                FROM term_dict, term_doc 
                WHERE term_dict.term_id = term_doc.term_id 
                GROUP BY term_dict.term_id, df, string 
                ORDER BY string 
            """)
            for output in self.cursor.fetchall():
                postingsList = PostingsList()
                df, term, postings = output
                assert len(postings) == df
                cf = sum(p['tf'] for p in postings)
                postingsList.term = term
                postingsList.df = df
                postingsList.cf = cf
                old_id = 0
                for p in postings:
                    posting = Posting()
                    doc_id = p['doc_id']
                    tf = p['tf']
                    posting.docid = doc_id - old_id
                    old_id = doc_id
                    posting.tf = tf
                    postingsList.postings.append(posting)
                f.write_message(postingsList)

            # Create doc records
            self.cursor.execute("""
                SELECT doc_id, collection_id, len
                FROM docs
                ORDER BY doc_id
            """)
            for output in self.cursor.fetchall():
                docRecord = DocRecord()
                doc_id, collection_id, length = output
                docRecord.docid = doc_id
                docRecord.collection_docid = collection_id
                docRecord.doclength = length
                f.write_message(docRecord)


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('--database', required=True)
    parser.add_argument('--ciff', required=True)
    parser.add_argument('--docs')
    parser.add_argument('--term_dict')
    parser.add_argument('--term_doc')
    parser.add_argument('--delimiter')
    args = parser.parse_args()
    ToCiff(**vars(args))
