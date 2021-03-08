def _create_table(connection, table_name, column_names, column_types):
    cursor = connection.cursor()
    try:
        cursor.execute(f'SELECT * FROM {table_name} LIMIT 1;')
        connection.rollback()
        raise IOError('Table already exists.')
    except RuntimeError:
        pass
    query = f'CREATE TABLE {table_name} ({", ".join([f"{a} {b}" for a, b in zip(column_names, column_types)])});'
    cursor.execute(query)


def _fill_empty_table_with_csv(connection, table_name, file_name, delimiter="|"):
    cursor = connection.cursor()
    cursor.execute(f'SELECT COUNT(*) FROM {table_name};')
    if cursor.fetchone()[0] > 0:
        connection.rollback()
        raise IOError('The tables are not empty.')
    query = f"COPY {table_name} FROM '{file_name}' WITH DELIMITER '{delimiter}';"
    cursor.execute(query)
