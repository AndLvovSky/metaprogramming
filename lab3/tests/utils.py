import psycopg2
import logging


def _get_connection(db_config):
    return psycopg2.connect(**db_config)


def execute(db_config, query):
    logging.debug('Test query:')
    logging.debug(query)
    connection = _get_connection(db_config)
    with connection.cursor() as cursor:
        cursor.execute(query)
    connection.commit()
    connection.close()


def select_all(db_config, query):
    connection = _get_connection(db_config)
    with connection.cursor() as cursor:
        cursor.execute(query)
        values = cursor.fetchall()
    connection.close()
    return values


def get_table_records(db_config, table_name, fields):
    query = f"""
        select {', '.join([field for field in fields])} from {table_name}
        order by {fields[0]}
    """
    logging.debug('Test query:')
    logging.debug(query)
    return select_all(db_config, query)


def drop_all_tables(db_config):
    query = """
        drop schema public cascade;
        create schema public
    """
    execute(db_config, query)


def table_structure_matches(expected, actual):
    actual = set([(table[1], table[2]) for table in actual])
    return expected == actual
