import psycopg2
import logging

class Py2SQL:
    @property
    def connection(self):
        if not hasattr(self, '_connection'):
            raise Exception('No connection is established, call db_connect first')
        return self._connection

    def db_connect(self, **config):
        if hasattr(self, '_connection'):
            raise Exception('Connection is already established')
        self._connection = psycopg2.connect(**config)
        logging.info('Database connection is established')

    def db_disconnect(self):
        self.connection.close()
        del self._connection
        logging.info('Database connection is closed')

    @property
    def db_engine(self):
        return self._select_single('select version()')

    @property
    def db_name(self):
        return self._select_single('select current_database()')

    @property
    def db_size(self):
        size = self._select_single(f"select pg_size_pretty(pg_database_size('{self.db_name}'))")
        return self._size_kb_to_mb(size)

    @property
    def db_tables(self):
        tables = self._select_all("""
            select tablename 
            from pg_catalog.pg_tables
            where schemaname = 'public'
        """)
        return [table[0] for table in tables]

    def db_table_structure(self, name):
        self._check_table_exists(name)
        return self._select_all(f"""
            select ordinal_position, column_name, data_type 
            from INFORMATION_SCHEMA.COLUMNS 
            where table_name = '{name}'
            order by ordinal_position
        """)

    def db_table_size(self, name):
        self._check_table_exists(name)
        size = self._select_single(f"select pg_size_pretty(pg_total_relation_size('{name}'))")
        return self._size_kb_to_mb(size)

    def _select_all(self, query):
        cursor = self.connection.cursor()
        cursor.execute(query)
        return cursor.fetchall()

    def _select_single(self, query):
        cursor = self.connection.cursor()
        cursor.execute(query)
        return cursor.fetchone()[0]

    def _size_kb_to_mb(self, size):
        return float(size.split(' ')[0]) / 1000

    def _check_table_exists(self, name):
        if name not in self.db_tables:
            raise Exception(f'Table {name} does not exist in schema public')
