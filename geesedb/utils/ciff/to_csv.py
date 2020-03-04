#! /usr/bin/env python3.6

import argparse
import sys

from . import ciff_pb2


class ToCSV:
    """
    Class for creating csv files that represent tables in the old dog paper:
    - https://dl.acm.org/doi/10.1145/2600428.2609460
    """
    def __init__(self, **kwargs):
        self.arguments = self.get_arguments(kwargs)
        self.postings_file = self.arguments['protobuf_file']
        self.docs_file = self.arguments['docs_file']
        if self.postings_file:
            self.create_term_files(self.arguments['output_term_dict'], self.arguments['output_term_doc'])
        if self.docs_file:
            self.create_doc_file(self.arguments['output_docs'])

    @staticmethod
    def get_arguments(kwargs):
        arguments = {
            'protobuf_file': None,
            'docs_file': None,
            'output_docs': None,
            'output_term_dict': None,
            'output_term_doc': None
        }
        for key, item in arguments.items():
            if kwargs.get(key) is not None:
                arguments[key] = kwargs.get(key)
        # TODO Some combinations are required, however wait until new CIFF version to change it.
        return arguments

    @staticmethod
    def decode(buffer, pos):
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

    def create_term_files(self, out_term_dict, out_term_doc):
        with open(self.postings_file, 'rb') as f:
            data = f.read()

            term_dict_writer = open(out_term_dict, 'w')
            term_doc_writer = open(out_term_doc, 'w')

            next_pos, pos = 0, 0
            term_id = 0
            while pos < len(data):
                posting_list = ciff_pb2.PostingsList()
                next_pos, pos = self.decode(data, pos)
                posting_list.ParseFromString(data[pos:pos+next_pos])
                pos += next_pos

                term_dict_writer.write(f'{term_id}|{posting_list.df}|{posting_list.term}\n')
                for posting in posting_list.posting:
                    term_doc_writer.write(f'{term_id}|{posting.docid}|{posting.tf}\n')
                term_id += 1
            term_dict_writer.close()
            term_doc_writer.close()

    def create_doc_file(self, out_doc):
        with open(self.docs_file, 'r') as f:
            doc_writer = open(out_doc, 'w')
            for line in f:
                doc_id, trec_id, length = line.strip().split("\t")
                doc_writer.write(f'{trec_id}|{doc_id}|{length}\n')


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('-p',
                        '--protobuf_file',
                        required='--output_term_dict' in sys.argv and
                                 '--output_term_doc' in sys.argv,
                        metavar='[file]',
                        help='Location of the protobuf file, if this is included ' +
                             'output files for term related files should also be specified.')
    parser.add_argument('-d',
                        '--docs_file',
                        required='--output_term_doc' in sys.argv,
                        metavar='[file]',
                        help='Location of the document metadata file, if this is included ' +
                             'the output file for documents should also be specified.')
    parser.add_argument('-o',
                        '--output_docs',
                        required='--docs_file' in sys.argv,
                        metavar='[file]',
                        help='Output csv file for the docs table.')
    parser.add_argument('-t',
                        '--output_term_dict',
                        required='--protobuf_file' in sys.argv,
                        metavar='[file]',
                        help='Output csv file for the term dictionary table.')
    parser.add_argument('-e',
                        '--output_term_doc',
                        required='--protobuf_file' in sys.argv,
                        metavar='[file]',
                        help='Output csv file for the term doc mapper table.')
    ToCSV(**vars(parser.parse_args()))
