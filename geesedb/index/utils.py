import numpy as np
from typing import List
from duckdb import DuckDBPyConnection, CatalogException


def _create_table(connection: DuckDBPyConnection, table_name: str, column_names: List[str],
                  column_types: List[str]) -> None:
    cursor = connection.cursor()
    try:
        cursor.execute(f'SELECT * FROM {table_name} LIMIT 1;')
        connection.rollback()
        raise IOError('Table already exists.')
    except RuntimeError or IOError or CatalogException:
        pass
    query = f'CREATE TABLE {table_name} ({", ".join([f"{a} {b}" for a, b in zip(column_names, column_types)])});'
    cursor.execute(query)


def _fill_empty_table_with_csv(connection: DuckDBPyConnection, table_name: str, file_name: str,
                               delimiter: str = "|") -> None:
    cursor = connection.cursor()
    cursor.execute(f'SELECT COUNT(*) FROM {table_name};')
    if cursor.fetchone()[0] > 0:
        connection.rollback()
        raise IOError('The tables are not empty.')
    print(table_name)
    query = f"COPY {table_name} FROM '{file_name}' WITH DELIMITER '{delimiter}';"
    cursor.execute(query)


def _create_and_fill_empty_table_with_pd(connection: DuckDBPyConnection, table_name: str, pd_dataframe) -> None:
    cursor = connection.cursor()
    cursor.execute(f"DROP TABLE IF EXISTS {table_name};")
    cursor.register(table_name, pd_dataframe)
    cursor.execute(f"CREATE TABLE {table_name} AS SELECT * FROM {table_name};")


def _create_and_fill_metadata_table(connection: DuckDBPyConnection, table_name: str) -> None:
    cursor = connection.cursor()

    # create metadata table and insert distinct values
    cursor.execute(f'DROP TABLE IF EXISTS {table_name}_table;')
    cursor.execute("DROP SEQUENCE IF EXISTS seq_met_id;")
    cursor.execute("CREATE SEQUENCE seq_met_id START 1;")
    cursor.execute(f"CREATE TABLE {table_name}_table AS "
                   f"SELECT nextval('seq_met_id') AS '{table_name}_id', a.* "
                   f"FROM("
                   f"SELECT DISTINCT json_object.json->>'{table_name}' AS 'str' "
                   "FROM json_object) a;")
    cursor.execute("DROP SEQUENCE seq_met_id;")

    # update docs table with new id
    cursor.execute("ALTER TABLE docs "
                   f"ADD COLUMN {table_name}_id INT;")
    cursor.execute(f"UPDATE docs "
                   f"SET {table_name}_id = a.{table_name}_id "
                   f"FROM("
                   f"SELECT m.{table_name}_id as '{table_name}_id', j.doc_id as 'doc_id' "
                   f"FROM (select doc_id, json->>'{table_name}' as {table_name} from json_object) j "
                   f"INNER JOIN {table_name}_table m ON m.str = j.{table_name}) a "
                   f"WHERE docs.doc_id = a.doc_id;")


def _read_json_file(connection: DuckDBPyConnection, file_path: str) -> (int, {}):
    cursor = connection.cursor()
    cursor.install_extension('json')
    cursor.load_extension('json')

    cursor.execute("DROP TABLE IF EXISTS mem_terms_table;")
    cursor.execute("CREATE TABLE mem_terms_table (str VARCHAR, doc_id INT);")
    cursor.execute("DROP SEQUENCE IF EXISTS seq_doc_id;")
    cursor.execute("CREATE SEQUENCE seq_doc_id START 1;")
    cursor.execute("DROP TABLE IF EXISTS json_object;")
    cursor.execute(f"CREATE TABLE json_object AS "
                   f"SELECT nextval('seq_doc_id') as 'doc_id', j.* FROM read_ndjson_objects('{file_path}') j;")
    cursor.execute("DROP SEQUENCE seq_doc_id;")

    cursor.execute(f"SELECT COUNT(*) FROM json_object;")
    doc_rows = cursor.fetchone()

    cursor.execute(f"SELECT * FROM read_ndjson_objects('{file_path}');")
    raw_dict_line = cursor.fetchone()

    return doc_rows[0], raw_dict_line[0]


def _create_main_tables(connection: DuckDBPyConnection, doc_rows: int, collection_id_field_name=None) -> None:
    cursor = connection.cursor()

    cursor.execute("DROP SEQUENCE IF EXISTS seq_doc_id;")
    cursor.execute("CREATE SEQUENCE seq_doc_id START 1;")
    cursor.execute("DROP TABLE IF EXISTS docs;")
    cursor.execute("CREATE TABLE docs (collection_id VARCHAR, doc_id INT, len INT);")
    if collection_id_field_name is not None:
        cursor.execute("INSERT INTO docs (collection_id, doc_id)"
                       f"SELECT j.json->>'{collection_id_field_name}', j.doc_id "
                       "FROM json_object j;")
    else:
        inserts = '(' + '), ('.join(map(str, np.arange(1, doc_rows+1))) + ')'
        cursor.execute(f"INSERT INTO docs (doc_id) VALUES {inserts};")
    cursor.execute("DROP SEQUENCE seq_doc_id;")

    cursor.execute('DROP TABLE IF EXISTS term_dict;')
    cursor.execute("CREATE TABLE term_dict (term_id INT not null, str VARCHAR, df INT, PRIMARY KEY (term_id));")

    cursor.execute("DROP SEQUENCE IF EXISTS seq_term_id;")
    cursor.execute("CREATE SEQUENCE seq_term_id START 1;")
    cursor.execute('DROP TABLE IF EXISTS term_doc;')
    cursor.execute("CREATE TABLE term_doc (term_id INT, doc_id INT, tf INT);")


def _get_indexable_line(connection: DuckDBPyConnection, indexable_column_names: str, batch_size: int, i: int) -> []:
    cursor = connection.cursor()
    cursor.execute(f"SELECT CONCAT {indexable_column_names} FROM json_object LIMIT {batch_size} OFFSET {i*batch_size};")
    return cursor.fetchall()


def _alter_docs_table(connection: DuckDBPyConnection, df) -> None:
    cursor = connection.cursor()
    cursor.execute("UPDATE docs d "
                   "SET len = df.len "
                   "FROM df "
                   "WHERE d.doc_id = df.doc_id;")


def _update_mem_terms_table(connection: DuckDBPyConnection, query: str) -> None:
    cursor = connection.cursor()
    cursor.execute(f"INSERT INTO mem_terms_table (str, doc_id) VALUES {query};")
    # val = [[word, doc_id] for word in words_list]
    # cursor.executemany("INSERT INTO mem_terms_table (str, doc_id) VALUES (?, ?)", val)


def _create_final_tables(connection: DuckDBPyConnection) -> None:
    cursor = connection.cursor()
    cursor.execute("INSERT INTO term_dict "  
                   "SELECT nextval('seq_term_id'), m.str, COUNT(*) AS 'df' "
                   "FROM mem_terms_table m "
                   "GROUP BY str;")
    cursor.execute("INSERT INTO term_doc(term_id, doc_id, tf) "
                   "SELECT t.term_id, m.doc_id, COUNT(*) "
                   "FROM mem_terms_table m "
                   "JOIN term_dict t ON m.str = t.str "
                   "GROUP BY t.term_id, m.doc_id;")
    cursor.execute("DROP TABLE mem_terms_table;")
    cursor.execute("DROP TABLE json_object;")


def _get_all_values_from_json_key(connection: DuckDBPyConnection, key_name: str) -> []:
    cursor = connection.cursor()
    cursor.execute(f"SELECT json->'$.{key_name}' FROM json_object;")
    return cursor.fetchall()
