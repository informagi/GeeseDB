import pycypher
from .metadata import Metadata

class Parser:

    def __init__(self, database):
        self.parseCypher = _ParseCypher(database)

    def parse(self, cypher_query):
        node = pycypher.parse(cypher_query)
        return self.parseCypher.process_node(node)

class _ParseCypher:

    def __init__(self, database):
        self.database = database

    def process_node(self, node):
        errors = node['errors']
        name = node['name']
        result = node['result']

        if len(errors) > 0:
            print(f"There are errors in the query:")
            print(errors)
            raise RuntimeError

        if name == 'Cypher':
            for r in result:
                try:
                    if r['children']['name'] == 'Statement':
                        return self.process_node(r['children'])
                except KeyError:
                    continue

        elif name == 'Statement':
            return self.process_node(result[0]['children'])

        elif name == 'Query':
            return self.process_node(result[0]['children'])

        elif name == 'StandaloneQuery':
            raise RuntimeError("We don not support StandaloneQuery queries (yet).")

        elif name == 'RegularQuery':
            out = ''
            for r in result:
                if r['node'] == r['children']:
                    continue
                if len(out) > 0:
                    out += ' '
                if r['children']['name'] == 'SingleQuery':
                    out += _ParseSingleQuery(self.database).process_node(r['children'])
                else:
                    out += self.process_node(r['children'])
            return out

        elif name == 'Union':
            union = ''
            for r in result[:-1]:
                union += r['node']['text']
            union = union.strip()
            return union + ' ' + _ParseSingleQuery(self.database).process_node(result[-1]['children'])

        else:
            raise RuntimeError(f'Queries that make use of >>{name}<< are not supported (yet).')

class _ParseSingleQuery:

    def __init__(self, database):
        self.output_params = {
            "Order": '',
            "Skip": '',
            "Limit": ''
        }
        self.additional_wheres = list()
        self.metadata = Metadata(database)

    def build_select_statement(self, pattern):
        output = ''

        # First the start node
        s_node = pattern['NodePattern'][0]
        try:
            s_variable = s_node['Variable']
        except KeyError:
            s_variable = 'start_node'

        try:
            s_label = s_node['NodeLabels']
        except KeyError:
            raise RuntimeError('The type of a node needs to be know for know')

        try:
            s_properties = s_node['Properties']
            for key, value in s_properties.items():
                self.additional_wheres.append(f"""{s_variable}.{key} = {value.replace('"', "'")}""")
        except KeyError:
            pass

        output += f'{s_label} AS {s_variable}'

        # Then the chain
        try:
            chain = pattern['PatternElementChain']
        except KeyError:
            return output

        p_label = s_label
        p_variable = s_variable
        for i, chain_part in enumerate(chain):
            to_node = chain_part['node']
            try:
                to_node_variable = to_node['Variable']
            except KeyError:
                to_node_variable = f'Xtn{i}X'
            try:
                to_node_type = to_node['NodeLabels']
            except KeyError:
                raise RuntimeError("The node type needs to be known for now.")
            try:
                to_node_properties = to_node['Properties']
                for key, value in to_node_properties.items():
                    self.additional_wheres.append(f"""{to_node_variable}.{key} = {value.replace('"', "'")}""")
            except KeyError:
                pass

            relationship = chain_part['relationship']
            try:
                rel_variable = relationship['Variable']
            except KeyError:
                rel_variable = f'Xrel{i}X'
            try:
                rel_type = relationship['RelationshipTypes'][0]
            except KeyError:
                rel_type = self.metadata.get_default_join_info(p_label, to_node_type)[0]
            try:
                rel_properties = relationship['Properties']
                for key, value in rel_properties.items():
                    self.additional_wheres.append(f"""{rel_variable}.{key} = {value.replace('"', "'")}""")
            except KeyError:
                pass
            meta = self.metadata.get_all_join_info(p_label, to_node_type)
            if not meta:
                raise RuntimeError(f"There are no edges between these node types known: {p_label} and {to_node_type}")
            meta = meta[0] # TODO unless join table is specified
            join_table, from_node_jk, join_table_fnk, join_table_tnk, to_node_jk = meta

            # Add relationship join and then the node join
            join = f' JOIN {join_table} AS {rel_variable} ON {p_variable}.{from_node_jk} = {rel_variable}.{join_table_fnk}' + \
                   f' JOIN {to_node_type} AS {to_node_variable} ON {rel_variable}.{join_table_tnk} = {to_node_variable}.{to_node_jk}'
            output += join
            p_variable = to_node_variable
            p_label = to_node_type
        return output

    def process_node(self, node):
        name = node['name']
        result = node['result']

        if name == 'SingleQuery':
            return self.process_node(result[0]['children'])

        elif name == 'SinglePartQuery':
            read_part = ''
            return_part = ''
            for r in result:
                if r['node'] == r['children']:
                    continue
                elif r['children']['name'] == 'UpdatingClause':
                    raise RuntimeError('Updates are not supported yet')
                elif r['children']['name'] == 'ReadingClause':
                    if len(read_part) > 0:
                        raise RuntimeError('Only one reading clause per query is supported')
                    read_part = self.process_node(r['children'])
                else:
                    return_part = self.process_node(r['children'])
            return return_part + ' ' + read_part \
                   + self.output_params['Order'] \
                   + self.output_params['Skip'] \
                   + self.output_params['Limit']

        elif name == 'ReadingClause':
            return self.process_node(result[0]['children'])

        elif name == 'Match':
            match_text = ''
            where = ''

            result_generator = (r for r in result)
            r = next(result_generator)
            while r['node'] == r['children']:
                match_text += r['node']['text']
                r = next(result_generator)
            if match_text.strip().upper().startswith('OPTIONAL'):
                raise RuntimeError('For now we do not support OPTIONAL matches yet.')
            pattern = self.process_node(r['children'])
            while True:
                try:
                    r = next(result_generator)
                    if r['node'] == r['children']:
                        continue
                    if r['children']['name'] == 'Where':
                        where = self.process_node(r['children'])
                except StopIteration:
                    break
            match_statement = f'FROM {pattern}'
            if len(where) == 0 and len(self.additional_wheres) > 0:
                where = ' WHERE ' + ' AND '.join(self.additional_wheres)
            elif len(self.additional_wheres) > 0:
                additional_and = ' AND ' + ' AND '.join(self.additional_wheres)
                where += additional_and
            if len(where) > 0:
                match_statement += where
            return match_statement

        elif name == 'Pattern':
            return_expression = ''
            for r in result:
                if r['node'] == r['children']:
                    return_expression += r['node']['text']
                else:
                    return_expression += self.process_node(r['children'])
            return return_expression

        elif name == 'PatternPart':
            return_expression = ''
            for r in result:
                if r['node'] == r['children']:
                    return_expression += r['node']['text']
                elif r['children']['name'] == 'Variable':
                    raise RuntimeError('Variable assignment of patterns is not supported yet.')
                else:
                    return_expression += self.process_node(r['children'])
            return return_expression

        elif name == 'AnonymousPatternPart':
            return ''.join([self.process_node(r['children']) for r in result])

        elif name == 'PatternElement':
            # Get processed chain data
            pattern = dict()
            for r in result:
                if r['node'] == r['children']:
                    continue
                else:
                    try:
                        pattern[r['children']['name']].append(self.process_node(r['children']))
                    except KeyError:
                        pattern[r['children']['name']] = [self.process_node(r['children'])]

            return self.build_select_statement(pattern)

        elif name == 'NodePattern':
            node = dict()
            for r in result:
                if r['node'] == r['children']:
                    continue
                else:
                    node[r['children']['name']] = self.process_node(r['children'])
            return node

        elif name == 'NodeLabels':
            if len(result) > 1:
                raise RuntimeError("Only one node label at a time is supported")
            return self.process_node(result[0]['children'])

        elif name == 'NodeLabel':
            node_label = ''
            for r in result:
                if r['node'] == r['children']:
                    continue
                else:
                    node_label = self.process_node(r['children'])
            return node_label

        elif name == 'LabelName':
            return self.process_node(result[0]['children'])

        elif name == 'Properties':
            return self.process_node(result[0]['children'])

        elif name == 'MapLiteral':
            map_literal = dict()
            key = None
            for r in result:
                if r['node'] == r['children']:
                    continue
                elif r['children']['name'] == 'PropertyKeyName':
                    key = self.process_node(r['children'])
                elif r['children']['name'] == 'Expression':
                    map_literal[key] = self.process_node(r['children'])
                    key = None
            return map_literal

        elif name == 'PatternElementChain':
            relationship = None
            node = None
            for r in result:
                if r['node'] == r['children']:
                    continue
                elif r['children']['name'] == 'RelationshipPattern':
                    relationship = self.process_node(r['children'])
                else:
                    node = self.process_node(r['children'])
            return {'relationship': relationship, 'node': node}

        elif name == 'RelationshipPattern':
            for r in result:
                if r['children']['name'] == 'Dash':
                    continue
                elif r['children']['name'] in {'LeftArrowHead', 'RightArrowHead'}:
                    raise RuntimeError('Directed edges are not supported yet.')
                else:
                    return self.process_node(r['children'])
            raise RuntimeError("RelationshipPattern should return a pattern")

        elif name == 'RelationshipDetail':
            relation = dict()
            for r in result:
                if r['node'] == r['children']:
                    continue
                else:
                    relation[r['children']['name']] = self.process_node(r['children'])
            return relation

        elif name == 'RelationshipTypes':
            relationship_types = []
            for r in result:
                if r['node'] == r['children']:
                    continue
                else:
                    relationship_types.append(self.process_node(r['children']))
            if len(relationship_types) > 1:
                raise RuntimeError("We only support one join table at a time for now.")
            return relationship_types

        elif name == 'RelTypeName':
            return self.process_node(result[0]['children'])

        elif name == 'Where':
            where_statement = ' '
            for r in result:
                if r['node'] == r['children']:
                    where_statement += r['node']['text']
                else:
                    where_statement += self.process_node(r['children'])
            return where_statement

        elif name == 'Return':
            out = ''
            for r in result[1:]:
                if r['node'] == r['children']:
                    if len(r['node']['text'].strip()) == 0:
                        continue
                    out += r['node']['text'].strip() + ' '
                else:
                    out += self.process_node(r['children'])
            return 'SELECT ' + out

        elif name == 'ReturnBody':
            out = ''
            for r in result:
                if r['node'] == r['children']:
                    continue
                elif r['children']['name'] == 'ReturnItems':
                    out = self.process_node(r['children'])
                elif r['children']['name'] in {'Order', 'Skip', 'Limit'}:
                    self.output_params[r['children']['name']] = ' ' + r['node']['text']
                else:
                    n = r['children']['name']
                    raise RuntimeError(f'Queries that make use of >>{n}<< are not supported (yet).')
            return out

        elif name == 'ReturnItems':
            return_items = ''
            for r in result:
                return_items += r['node']['text']
            # TODO
            # For now we just assume the ReturnItems is already correct, should make it better such
            # that e.g. nodes can be selected directly (now specific attributes have to be specified).
            return return_items

        elif name == 'Expression':
            for r in result:
                print(r)
            return self.process_node(result[0]['children'])

        elif name in {'OrExpression', 'AndExpression', 'XorExpression', 'NotExpression'}:
            keyword = name[:-10]
            expressions = []
            for r in result:
                if r['node'] == r['children']:
                    continue
                else:
                    expressions.append(self.process_node(r['children']))
            if len(expressions) == 1:
                return expressions[0]
            else:
                return f' {keyword.upper()} '.join(expressions)

        elif name == 'ComparisonExpression':
            possible_comparisons = {'=', '<>', '<', '>', '<=', '>='}
            comparisons = []
            for r in result:
                if r['node'] == r['children']:
                    continue
                else:
                    comparisons.append(self.process_node(r['children']))
            if len(comparisons) == 1:
                return comparisons[0]
            elif len(comparisons) == 2:
                return comparisons[0]  + ' ' + comparisons[1]
            else:
                comparison_expressions_unprocessed = []
                comparison_expressions = []
                for i in range(len(comparisons)-1):
                    comparison_expressions_unprocessed.append([comparisons[i], comparisons[i+1]])
                for expression_duo in comparison_expressions_unprocessed:
                    p1, p2 = expression_duo
                    for p in possible_comparisons:
                        p1 = p1.replace(p, '')
                    comparison_expressions.append(p1.strip() + ' ' + p2.strip())
                return ' AND '.join(comparison_expressions)

        elif name == 'PartialComparisonExpression':
            partial_comparison = ''
            for r in result:
                if r['node'] == r['children']:
                    partial_comparison += r['node']['text']
                else:
                    partial_comparison += self.process_node(r['children'])
            return partial_comparison

        elif name in {'AddOrSubtractExpression', 'MultiplyDivideModuloExpression',
                      'PowerOfExpression', 'UnaryAddOrSubtractExpression'}:
            return_expression= ''
            for r in result:
                if r['node'] == r['children']:
                    return_expression += r['node']['text']
                else:
                    return_expression += self.process_node(r['children'])
            return return_expression

        elif name == 'StringListNullOperatorExpression':
            return ' '.join([self.process_node(r['children']) for r in result])

        elif name == 'NullOperatorExpression':
            return ''.join([r['node']['text'] for r in result]).strip()

        elif name == 'PropertyOrLabelsExpression':
            return ''.join([self.process_node(r['children']) for r in result])

        elif name == 'PropertyLookup':
            return_expression = ''
            for r in result:
                if r['node'] == r['children']:
                    return_expression += r['node']['text']
                else:
                    return_expression += self.process_node(r['children'])
            return return_expression

        elif name == 'SchemaName':
            return_expression = ''
            for r in result:
                if r['node'] == r['children']:
                    return_expression += r['node']['text']
                else:
                    return_expression += self.process_node(r['children'])
            return return_expression

        elif name == 'PropertyKeyName':
            return_expression = ''
            for r in result:
                if r['node'] == r['children']:
                    return_expression += r['node']['text']
                else:
                    return_expression += self.process_node(r['children'])
            return return_expression

        elif name == 'Atom':
            return_expression = ''
            for r in result:
                print(f'Atom: {r}')
                print()
                if r['node'] == r['children']:
                    return_expression += r['node']['text']
                else:
                    return_expression += self.process_node(r['children'])
            return return_expression

        elif name == 'FunctionInvocation':
            return_expression = ''
            for r in result:
                print(f'Function inv: {r}')
                print()
                if r['node'] == r['children']:
                    return_expression += r['node']['text']
                elif r['children']['name'] == 'FunctionName':
                    return_expression += r['node']['text']
                else:
                    return_expression += self.process_node(r['children'])
            return return_expression

        elif name == 'Literal':
            return_expression = ''
            for r in result:
                if r['node'] == r['children']:
                    return_expression += r['node']['text'].replace('"', "'")
                else:
                    return_expression += self.process_node(r['children'])
            return return_expression

        elif name == 'NumberLiteral':
            return self.process_node(result[0]['children'])

        elif name == 'DoubleLiteral':
            return ''.join([r['node']['text'] for r in result]).strip()

        elif name == 'IntegerLiteral':
            return ''.join([r['node']['text'] for r in result]).strip()

        elif name == 'Variable':
            return self.process_node(result[0]['children'])

        elif name == 'SymbolicName':
            return ''.join([r['node']['text'] for r in result]).strip()

        elif name == 'ParenthesizedExpression':
            return_expression = ''
            for r in result:
                if r['node'] == r['children']:
                    return_expression += r['node']['text']
                else:
                    return_expression += self.process_node(r['children'])
            return return_expression

        elif name == 'MultiPartQueries':
            raise RuntimeError('The keyword WITH is not supported (yet).')

        else:
            raise RuntimeError(f'Queries that make use of >>{name}<< are not supported (yet).')