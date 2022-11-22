from geesedb.search import Searcher

import pandas as pd
import numpy as np


def get_topics_wp_trec(file_name: str):
    # return an array with topic number and string
    with open(file_name) as topics_file:
        nums = []
        strs = []
        str_file = topics_file.readlines()
        for i, line in enumerate(str_file):
            if '<num>' in line:
                nums.append(line.removeprefix('<num> Number: ').removesuffix(' </num>\n'))
            elif '<title>\n' in line:
                strs.append(str_file[i+1].removesuffix(' \n'))
        return nums, strs


def save_run_file(processor, database: str, topics_file: str, save_loc: str, n: int) -> None:
    """
    Create a file with n first retrieved documents using the provided duckDB database and a topics file (id and string)
    according to the TREC format.
    """
    searcher = Searcher(database=database, n=n)
    nums, strs = get_topics_wp_trec(topics_file)
    final = {'topic': [],
             'sad': [],
             'id': [],
             'rank': [],
             'score': [],
             'run_tag': []}
    q = ['Q0']*n
    run_tag = ['geesedb']*n
    rank = list(range(n+1))
    del rank[0]
    for top, s in zip(nums, strs):
        temp = searcher.search_topic(' '.join(processor(s))).to_dict()
        #print(temp)
        final['topic'].extend([top]*n)
        final['sad'].extend(q)
        final['id'].extend(temp['collection_id'].values())
        final['rank'].extend(rank)
        final['score'].extend(temp['score'].values())
        final['run_tag'].extend(run_tag)

    df = pd.DataFrame(final)
    np.savetxt(save_loc, df.values, fmt='%s')
