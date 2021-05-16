#! /usr/bin/env python3

import argparse
import gzip
from typing import Union, Any, Tuple

from . import CommonIndexFileFormat_pb2 as Ciff


class ToCSV:
    """
    Class for creating csv files that represent tables in the old dog paper:
    - https://dl.acm.org/doi/10.1145/2600428.2609460

    The files are created from a CIFF as described in:
    - https://arxiv.org/abs/2003.08276
    """
    def __init__(self, **kwargs: Any) -> None:
        self.arguments = self.get_arguments(kwargs)
        self.create_csv_files()

    @staticmethod
    def get_arguments(kwargs: Any) -> dict:
        arguments = {
            'protobuf_file': None,
            'output_docs': 'docs.csv',
            'output_term_dict': 'term_dict.csv',
            'output_term_doc': 'term_docs.csv'
        }
        for key, item in arguments.items():
            if kwargs.get(key) is not None:
                arguments[key] = kwargs.get(key)
        return arguments

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

    def create_csv_files(self) -> None:
        if self.arguments['protobuf_file'].endswith('.gz'):
            with gzip.open(self.arguments['protobuf_file'], 'rb') as f:
                data = f.read()
        else:
            with open(self.arguments['protobuf_file'], 'rb') as f:
                data = f.read()
        next_pos, pos = 0, 0

        # start with reading header info
        header = Ciff.Header()
        next_pos, pos = self.decode(data, pos)
        header.ParseFromString(data[pos:pos+next_pos])
        pos += next_pos

        # read posting lists
        term_dict_writer = open(self.arguments['output_term_dict'], 'w')
        term_doc_writer = open(self.arguments['output_term_doc'], 'w')
        postings_list = Ciff.PostingsList()
        for term_id in range(header.num_postings_lists):
            next_pos, pos = self.decode(data, pos)
            postings_list.ParseFromString(data[pos:pos+next_pos])
            pos += next_pos
            term_dict_writer.write(f'{term_id}|{postings_list.df}|{postings_list.term}\n')
            for posting in postings_list.postings:
                term_doc_writer.write(f'{term_id}|{posting.docid}|{posting.tf}\n')
        term_dict_writer.close()
        term_doc_writer.close()

        # read doc information
        docs_writer = open(self.arguments['output_docs'], 'w')
        doc_record = Ciff.DocRecord()
        for n in range(header.num_docs):
            next_pos, pos = self.decode(data, pos)
            doc_record.ParseFromString(data[pos:pos+next_pos])
            pos += next_pos
            docs_writer.write(f'{doc_record.collection_docid}|{doc_record.docid}|{doc_record.doclength}\n')
        docs_writer.close()


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('-p',
                        '--protobuf_file',
                        required=True,
                        metavar='[file]',
                        help='Location of the protobuf file, if this is included ' +
                             'output files for term related files should also be specified.')
    parser.add_argument('-o',
                        '--output_docs',
                        metavar='[file]',
                        help='Output csv file for the docs table.')
    parser.add_argument('-t',
                        '--output_term_dict',
                        metavar='[file]',
                        help='Output csv file for the term dictionary table.')
    parser.add_argument('-e',
                        '--output_term_doc',
                        metavar='[file]',
                        help='Output csv file for the term doc mapper table.')
    ToCSV(**vars(parser.parse_args()))
