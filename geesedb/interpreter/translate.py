import re
import base64
from .metadata import Metadata


class Translator:
    patterns = {
        # match return
        'match_return': re.compile('MATCH +(\(.*\)) +(WHERE *(?:.*)(?: *AND *(?:.*))* +)?(RETURN .*)',
                                   re.IGNORECASE),  # basic match return

        # return patterns
        'split_return': re.compile('RETURN((?: +DISTINCT)? +[^ ]*) *(ORDER +BY +[^ ]*(?: +(?:DESC)?(?:ASC)?)?)? '
                                   '*(SKIP +[^ ]*)? *(LIMIT +[^ ]*)?', re.IGNORECASE),

        # match patterns
        'match1': re.compile('\A\(([^()]*)\)'),  # head section (first node)
        'match2': re.compile('(-\[]-\([^()]*\))'),  # tail section (edges + rest of nodes)
        'split_ending': re.compile('-\[([^()]*)]-\((.*)\)'),  # split edge and to_connect node
        'extract_filters': re.compile('\A([^{}]*){(.*)}\Z'),  # extract filter from node / edge

        # only chars and numbers
        'variable_name_filter': re.compile('\W')  # use to only keep letters + numbers
    }

    def __init__(self, database):
        self.metadata = Metadata(database)

    def translate(self, query):
        if self.patterns['match_return'].match(query):
            regex_match = self.patterns['match_return'].match(query)
            _match, _filters, _return = regex_match.groups()
            sql_query = self.construct_sql_match_filter_return(_match, _filters, _return)
        else:
            raise
        return sql_query

    def construct_sql_match_filter_return(self, _match, _filters, _return):
        head, tail = self.split_return(_return)
        sql_query = f'SELECT {head.strip()} '
        joins, extra_filters = self.process_match(_match)
        filters = self.process_filters(_filters, extra_filters)
        out = (((f'{sql_query.strip().strip()}' + f' {joins.strip()}').strip() + f' {filters.strip()}').strip()
               + f' {tail.strip()}').strip()
        return out

    def split_return(self, _return):
        ret, order_by, skip, limit = self.patterns['split_return'].match(_return).groups()
        if order_by is None:
            order_by = ''
        if skip is None:
            skip = ''
        if limit is None:
            limit = ''
        return ret, ((f'{order_by}'.strip() + f' {skip}').strip() + f' {limit}').strip()

    def create_variable_name(self, t, index):
        v = self.patterns['variable_name_filter'].sub('', base64.b64encode((t + str(index)).encode()).decode('ascii'))
        return "X" + str(v) + 'X'

    def build_joins(self, nodes, edges):
        joins = ''
        joins += f'FROM {nodes[0][1]} AS {nodes[0][0]} '
        for i in range(len(nodes) - 1):
            node1 = nodes[i]
            edge = edges[i]
            node2 = nodes[i + 1]
            meta = self.metadata.get_default_join_info(node1[1], node2[1])
            joins += f'JOIN {edge[1]} AS {edge[0]} ON ({node1[0]}.{meta[1]} = {edge[0]}.{meta[2]}) JOIN {node2[1]}' \
                     f' AS {node2[0]} ON ({edge[0]}.{meta[3]} = {node2[0]}.{meta[4]}) '
        return joins.strip()

    def process_filters(self, filters, extra_filters):
        if filters is not None:
            f = filters.replace('"', "'")
            for e in extra_filters:
                pre, post = e
                post = post.replace('"', "'")
                f += f' AND {pre} = {post}'
        elif len(extra_filters) > 0:
            pre, post = extra_filters[0]
            post = post.replace('"', "'")
            f = f'WHERE {pre} = {post}'
            for e in extra_filters[1:]:
                pre, post = e
                post = post.replace('"', "'")
                f += f' AND {pre} = {post}'
        else:
            f = ''
        return f

    def process_match(self, _match):
        beginning_match = self.patterns['match1'].match(_match)
        beginning = beginning_match.group()[1:-1]
        endings = [e for e in self.patterns['match2'].findall(_match[beginning_match.span()[1]:])]
        edges_nodes = [self.patterns['split_ending'].match(e).groups() for e in endings]
        try:
            edges, nodes = [list(z) for z in zip(*edges_nodes)]
        except ValueError:
            edges = nodes = []
        nodes.insert(0, beginning)

        processed_nodes = []
        processed_edges = []
        filters = []

        for i, n in enumerate(nodes):
            try:
                node_data, fs = self.patterns['extract_filters'].match(n).groups()
            except AttributeError:
                node_data, fs = n, []
            head, tail = node_data.split(':')
            if head == '':
                head = self.create_variable_name(tail, i)
            processed_nodes.append([head.strip(), tail.strip()])
            if len(fs) > 0:
                fs = [[head + '.' + a, b] for a, b in [f.strip().split(':') for f in fs.split(',')]]
                for f in fs:
                    filters.append(f)

        for i, e in enumerate(edges):
            try:
                edge_data, fs = self.patterns['extract_filters'].match(e).groups()
            except AttributeError:
                edge_data, fs = e, []
            try:
                head, tail = edge_data.split(':')
            except ValueError:
                head = tail = ''
            if tail == '':
                before_node = processed_nodes[i][1]
                after_node = processed_nodes[i + 1][1]
                tail = self.metadata.get_default_join_info(before_node, after_node)[0]
            if head == '':
                head = self.create_variable_name(tail, i)
            processed_edges.append([head.strip(), tail.strip()])
        return self.build_joins(processed_nodes, processed_edges), filters
