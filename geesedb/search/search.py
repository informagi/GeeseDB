#! /usr/bin/env python3.6

import argparse

from geesedb.connection.connection import DBConnection


class Searcher:

    @staticmethod
    def _parse_search_arguments():
        parser = argparse.ArgumentParser()
        parser.add_argument('--database', required=True, help='Name of the database / index')
        return parser.parse_args()

    def __init__(self):
        self.args = self._parse_search_arguments()
        self.db_connection = DBConnection(self.args.database)


if __name__ == '__main__':
    Searcher()
