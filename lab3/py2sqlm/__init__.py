import psycopg2
import logging
from py2sqlm.fields import get_class_database_fields, get_primary_key, ForeignKey

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

    def save_object(self, obj):
        if self._record_exists(obj):
            self._replace_object(obj)
        else:
            self._create_object(obj)

    def _create_object(self, obj):
        table_name, field_names, field_values = self._get_object_info(obj)
        query = f"""
            insert into {table_name} ({', '.join(field_names)}) 
            values ({', '.join([str(field_value) for field_value in field_values])})
        """
        logging.debug(query)
        self._execute(query)

    def _replace_object(self, obj):
        table_name, field_names, field_values = self._get_object_info(obj)
        query = f"""
            update {table_name}
            set {', '.join([f'{field[0]} = {field[1]}' for field in zip(field_names, field_values)])}
        """
        logging.debug(query)
        self._execute(query)

    def _get_object_info(self, obj):
        clz = obj.__class__
        table_name = clz._table_name
        fields = get_class_database_fields(clz)
        field_names = [field.name for field in fields]
        field_values = [getattr(obj, field.name) for field in fields]
        return (table_name, field_names, field_values)

    def _record_exists(self, obj):
        clz = obj.__class__
        self._check_table_exists_for_class(clz)
        primary_key = get_primary_key(clz)
        query = f"""
            select count(*) from {table} where {id_name} = {id_value}
        """
        logging.debug(query)
        count = self._select_single(query)
        return count == 1

    def save_class(self, clz):
        self._check_is_table(clz)
        table_name = clz._table_name
        if table_name in self.db_tables:
            self._update_class(clz)
        else:
            self._create_class(clz)

    def save_hierarchy(self, root_class):
        self._check_is_table(root_class)
        fields = get_class_database_fields(root_class)
        refererenced_tables = list(filter(lambda field: isinstance(field, ForeignKey), fields))
        for refererenced_table in refererenced_tables:
            self.save_hierarchy(refererenced_table.mapping_class)
        self.save_class(root_class)

    def _create_class(self, clz):
        fields = get_class_database_fields(clz)
        column_separator = ', \n\t\t\t\t'
        query = f"""
            create table {clz._table_name} (
                {column_separator.join([field.definition for field in fields])}  
            )  
        """
        logging.debug(query)
        self._execute(query)

    def _update_class(self, clz):
        field_names = set([field.name for field in get_class_database_fields(clz)])
        actual_field_names = set([field[1] for field in self.db_table_structure(clz._table_name)])
        field_names_to_add = []
        field_names_to_drop = []
        for field_name in field_names:
            if not field_name in actual_field_names:
                field_names_to_add.append(field_name)
        for field_name in actual_field_names:
            if not field_name in field_names:
                field_names_to_drop.append(field_name)
        self._add_columns(clz, field_names_to_add)
        self._drop_columns(clz, field_names_to_drop)

    def _add_columns(self, clz, field_names):
        if not field_names:
            return
        fields = [clz.__dict__[field_name] for field_name in field_names]
        delimiter = ', \n\t\t\t'
        query = f"""
            alter table {clz._table_name}
            {delimiter.join([f'add {field.definition}' for field in fields])}
        """
        logging.debug(query)
        self._execute(query)

    def _drop_columns(self, clz, field_names):
        if not field_names:
            return
        delimiter = ', \n\t\t\t'
        query = f"""
            alter table {clz._table_name}
            {delimiter.join([f'drop column {field_name}' for field_name in field_names])}
        """
        logging.debug(query)
        self._execute(query)

    def delete_object(self, obj):
        self._check_table_exists_for_class(obj.__class__)
        clz = obj.__class__
        primary_key = get_primary_key(clz)
        query = f"""
            delete from {clz._table_name} where id = {getattr(obj, primary_key.name)}
        """
        logging.debug(query)
        self._execute(query)

    def delete_class(self, clz):
        self._check_table_exists_for_class(clz)
        query = f"""
            drop table {clz._table_name}
        """
        logging.debug(query)
        self._execute(query)

    def delete_hierarchy(self, root_class):
        self._check_table_exists_for_class(root_class)
        fields = get_class_database_fields(root_class)
        refererenced_tables = list(filter(lambda field: isinstance(field, ForeignKey), fields))
        self.delete_class(root_class)
        for refererenced_table in refererenced_tables:
            self.delete_hierarchy(refererenced_table.mapping_class)

    def _select_all(self, query):
        with self.connection.cursor() as cursor:
            cursor.execute(query)
            values = cursor.fetchall()
        return values

    def _select_single(self, query):
        with self.connection.cursor() as cursor:
            cursor.execute(query)
            value = cursor.fetchone()[0]
        return value

    def _execute(self, query):
        with self.connection.cursor() as cursor:
            cursor.execute(query)
        self.connection.commit()

    def _size_kb_to_mb(self, size):
        return float(size.split(' ')[0]) / 1000

    def _check_table_exists(self, name):
        if name not in self.db_tables:
            raise Exception(f'Table {name} does not exist in schema public')

    def _check_is_table(self, clz):
        if not hasattr(clz, '_table_name'):
            raise Exception('Object class should have @table decorator')

    def _check_table_exists_for_class(self, clz):
        self._check_is_table(clz)
        self._check_table_exists(clz._table_name)
