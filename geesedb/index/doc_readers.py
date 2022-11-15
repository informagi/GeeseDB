from resiliparse.parse.html import HTMLTree
from resiliparse.extract.html2text import extract_plain_text

doc_general = {'collection_id': None,
               'doc_id': None,
               'title': '',
               'date': '',
               'authors': [],
               'content': '',
               'others': {}}


def read_from_WaPo_json(line: {}, doc_id: int, include_links: bool) -> doc_general:
    """
    Read input docs from JSONL file and convert it to the general format.
    """
    if line['id'] is None:
        raise IOError('Missing collection ID')
    d: doc_general = {'collection_id': line['id'], 'doc_id': doc_id, 'title': line['title'],
                      'date': line['published_date'], 'authors': [author for author in line['author'].split(',')]}
    # in case authors are separated by commas
    text = ''
    if 'contents' in line:
        for dict_ in line['contents']: #types: kicker, title, image (fullcaption), byline, sanitized_html
            if (dict_ is not None) and ('content' in dict_) and ('type' in dict_):
                if dict_['type'] == 'kicker' or dict_['type'] == 'title':
                    text += str(dict_['content']) + ' '
                elif dict_['type'] == 'sanitized_html':
                    text += str(extract_plain_text(HTMLTree.parse(dict_['content']), links=include_links, list_bullets=False)) + ' '
        d['content'] = text
    elif 'content' in line:
        d['content'] = line['content']
    else:
        raise IOError('Contents in the .jl file not found.')

    # for any other key not mentioned before, its content goes to others
    for k in line.keys():
        if k not in d.keys():
            d['others'] = line[k]

    return d
