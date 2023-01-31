from geesedb.resources.schema.names import names
from ..utils import _get_all_values_from_json_key

import time
import re
import random
import numpy as np
from typing import Dict, List
from dateutil.parser import parse as parse_time, ParserError
from duckdb import DuckDBPyConnection


# ----------------------------------------------------------------------------------------------------------------------
# Infer variable functions
# ----------------------------------------------------------------------------------------------------------------------

def infer_time(date_val, unix_now) -> bool:
    try:
        parse_time(str(date_val))
        return True
    except (OverflowError, ParserError, ValueError) as e:
        pass
    try:
        int(date_val)
    except (ValueError, TypeError) as e:
        return False
    if isinstance(date_val, int) and len(str(date_val)) in {10, 13, 16, 19}:
        if 0 < date_val < int(str(unix_now)[:len(str(date_val))]):
            return True
    else:
        return False


def infer_name(name_val, tokenizer_function, threshold=0.5) -> bool:
    # https://github.com/dangayle/first-name-gender/blob/master/names/names.py
    tokens = tokenizer_function(name_val)
    count = 0
    token_len = 0
    if isinstance(name_val, str):
        for word in tokens:
            token_len += 1
            if word.value.lower() in names:
                count += 1
    else:
        return False
    if token_len != 0 and count / token_len >= threshold:
        return True
    else:
        return False


def infer_ids(ids_list, std_threshold=1, list_rate=0.3) -> int:
    """
    0: not an id
    1: might be str. id
    2: might be increasing int id
    3: might be another kind of id, but less likely
    """
    # check if all values are unique
    if len(ids_list) != len(set(ids_list)):
        return 0

    # check the length distribution (better if there's little deviation)
    lengths = []
    int_type_count = 0
    ids_random_vals = random.sample(ids_list, int(list_rate * len(ids_list)))
    for val in ids_random_vals:
        lengths.append(len(val[0]))
        if isinstance(val[0], int):
            int_type_count += 1
    if int_type_count == 0 and np.std(
            lengths) <= std_threshold:  # look at distribution if it's str
        return 1
    elif int_type_count == len(ids_random_vals) and np.max(ids_list) - np.min(ids_list) == len(ids_list) - 1:
        return 2
    else:
        return 3


def infer_url(url_val) -> (bool, bool):  # is_url, is_string
    if isinstance(url_val, str):
        regex = r"(?i)\b((?:https?://|www\d{0,3}[.]|[a-z0-9.\-]+[.][a-z]{2,4}/)(?:[^\s()<>]+|\(([^\s()<>]+|(\([" \
                r"^\s()<>]+\)))*\))+(?:\(([^\s()<>]+|(\([^\s()<>]+\)))*\)|[^\s`!()\[\]{};:'\".,<>?«»“”‘’]))"
        url = re.findall(regex, url_val)
        if len(url) != 0:
            return True, True
        else:
            return False, True
    else:
        return False, False


# ----------------------------------------------------------------------------------------------------------------------
# Assess if value is important for the schema creation, an ID, or used for search
# ----------------------------------------------------------------------------------------------------------------------

def get_true_percentage(dict_list: List, upper_threshold_list=None, lower_threshold_list=None) -> bool:
    if upper_threshold_list is None:
        upper_threshold_list = [0.8, 0.8, 0.65]
    if lower_threshold_list is None:
        lower_threshold_list = [0.05, 0.05, 0.05]
    assert len(dict_list) == len(upper_threshold_list) == len(lower_threshold_list)
    count = 0
    for d, t_up, t_low in zip(dict_list, upper_threshold_list, lower_threshold_list):
        perc = d[True] / (d[True] + d[False])
        if perc >= t_up:
            return True
        elif perc < t_low:  # if all have really low percentages, return True to break loop
            count += 1
    if count == len(dict_list):
        return True
    return False


def infer_variable(connection: DuckDBPyConnection, dict_keys: List, tokenizer_function, schema_dict=None,
                   stop_step: int = 1000, max_step: int = 10000) -> Dict:
    """
    Calls all above functions for every variable.

    decisions for schema_dict include: <id>, <indexable>, <metadata>, <other>
    """
    unix_now = int(time.time_ns())
    decisions = {}
    id_present = False

    for key in dict_keys:
        if schema_dict is not None and key in schema_dict:
            if schema_dict[key] == '<id>':
                if id_present:
                    raise Exception("An ID has already been specified or inferred!")
                decisions[key] = schema_dict[key]
                id_present = True
            elif schema_dict[key] in ['<indexable>', '<metadata>', '<other>']:
                decisions[key] = schema_dict[key]
            else:
                decisions[key] = '<other>'
            continue

        val_list = _get_all_values_from_json_key(connection, key)
        url_dict = {True: 0, False: 0}
        time_dict = {True: 0, False: 0}
        names_dict = {True: 0, False: 0}
        is_string = {True: 0, False: 0}
        for i, val in enumerate(val_list):
            val = val[0][1:-1]  # delete quotes from reading from table
            is_url, is_string_bool = infer_url(val)
            url_dict[is_url] += 1
            is_string[is_string_bool] += 1
            time_dict[infer_time(val, unix_now)] += 1
            names_dict[infer_name(val, tokenizer_function)] += 1
            if i != 0 and i % stop_step == 0 and get_true_percentage([url_dict, time_dict, names_dict]):
                break
            if i == max_step:
                break
        url_dec = url_dict[True] / (url_dict[True] + url_dict[False])
        if url_dec >= 0.8:
            decisions[key] = '<url>'
            continue

        time_dec = time_dict[True] / (time_dict[True] + time_dict[False])
        if time_dec >= 0.8:
            decisions[key] = '<other>'
            continue

        names_dec = names_dict[True] / (names_dict[True] + names_dict[False])
        if names_dec >= 0.65:
            decisions[key] = '<metadata>'
            continue

        ids_int = infer_ids(val_list)
        if ids_int in [1, 2] and not id_present:
            id_present = True
            decisions[key] = '<doc_id>'
            continue
        elif ids_int == 3:
            decisions[key] = '<other>'
            continue

        is_string_dec = is_string[True] / (is_string[True] + is_string[False])
        if is_string_dec >= 0.95:  # not 100 adjusting for potential errors
            decisions[key] = '<indexable>'
        else:
            decisions[key] = '<other>'

    print('Final variable decisions:')
    print(decisions)
    return decisions
