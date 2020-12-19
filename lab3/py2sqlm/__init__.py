import logging
import psycopg2
import json
from functools import wraps
from py2sqlm.fields import *

def transactional(f):
    """
    Decorator for transactional methods.
    Wrapped method is executed in transaction and
    is rollbacked in case of a failure.

    :param f: transactional method
    :return: transactional wrapper
    """

    @wraps(f)
    def wrapper(self, *args, **kwargs):
        try:
            f(self, *args, **kwargs)
        except Exception as exc:
            self.connection.rollback()
            raise exc
        self.connection.commit()
    return wrapper

class Py2SQL:
    """
        Python to PostgreSQL mapper
    """

    @property
    def connection(self):
        """
        Database connection property
        """

        if not hasattr(self, '_connection'):
            raise Exception('No connection is established, call db_connect first')
        return self._connection

    def db_connect(self, **config):
        """
        Establish connection with database.
        Possible config fields: host, database, user, password, port
        """
        if hasattr(self, '_connection'):
            raise Exception('Connection is already established')
        self._connection = psycopg2.connect(**config)
        logging.info('Database connection is established')

    def db_disconnect(self):
        """
        Close database connection
        """
        self.connection.close()
        del self._connection
        logging.info('Database connection is closed')

    @property
    def db_engine(self):
        """
        :return: DBMS name and version
        """
        return self._select_single('select version()')

    @property
    def db_name(self):
        """
        :return: current database name
        """
        return self._select_single('select current_database()')

    @property
    def db_size(self):
        """
        :return: database size in Mb
        """
        size = self._select_single(f"select pg_size_pretty(pg_database_size('{self.db_name}'))")
        return self._size_kb_to_mb(size)

    @property
    def db_tables(self):
        """
        :return: all database table names in public schema
        """
        tables = self._select_all("""
            select tablename 
            from pg_catalog.pg_tables
            where schemaname = 'public'
        """)
        return sorted([table[0] for table in tables])

    def db_table_structure(self, name):
        """
        :return: database sctructure as list of tuples (id, name, type)
        """
        self._check_table_exists(name)
        return self._select_all(f"""
            select ordinal_position, column_name, data_type 
            from INFORMATION_SCHEMA.COLUMNS 
            where table_name = '{name}'
            order by ordinal_position
        """)

    def db_table_size(self, name):
        """
        :param name: table name
        :return: database table size in Mb
        """
        self._check_table_exists(name)
        size = self._select_single(f"select pg_size_pretty(pg_total_relation_size('{name}'))")
        return self._size_kb_to_mb(size)

    @transactional
    def save_object(self, obj):
        """
        Create or replace object and child objects in database
        :param obj: object to save
        """
        self._save_object(obj)

    def _save_object(self, obj):
        clz = obj.__class__
        self._check_table_exists_for_class(clz)
        fields = get_class_database_fields(clz)
        referenced_fields = list(filter(lambda field: isinstance(field, ForeignKey), fields))
        referenced_objects = [getattr(obj, referenced_table.name) for referenced_table in referenced_fields]
        fields_referenced_to = list(filter(lambda field: isinstance(field, ManyRelation), clz.__dict__.values()))
        objects_referenced_to = []
        for field_referenced_to in fields_referenced_to:
            objects_referenced_to += getattr(obj, field_referenced_to.name)

        for referenced_object in referenced_objects:
            self._save_object(referenced_object)
        if self._record_exists(obj):
            self._replace_object(obj)
        else:
            self._create_object(obj)
        for object_referenced_to in objects_referenced_to:
            self._save_object(object_referenced_to)

    def _create_object(self, obj):
        table_name, field_names, field_values = self._get_object_info(obj)
        query = f"""
            insert into {table_name} ({', '.join(field_names)}) 
            values ({', '.join([self._format_field(field_value) for field_value in field_values])})
        """
        logging.debug(query)
        self._execute(query)

    def _replace_object(self, obj):
        table_name, field_names, field_values = self._get_object_info(obj)
        primary_key = get_primary_key(obj.__class__)
        query = f"""
            update {table_name}
            set {', '.join([f'{field[0]} = {self._format_field(field[1])}' for field in zip(field_names, field_values)])}
            where {primary_key.name} = {getattr(obj, primary_key.name)}
        """
        logging.debug(query)
        self._execute(query)

    def _get_object_info(self, obj):
        clz = obj.__class__
        table_name = clz._table_name
        fields = get_class_database_fields(clz)
        field_names = [self._get_table_column_name(obj, field) for field in fields]
        field_values = [self._get_table_column_value(obj, field) for field in fields]
        return (table_name, field_names, field_values)

    def _get_table_column_name(self, obj, field):
        if isinstance(field, ForeignKey):
            return field.mapping_column
        return field.name

    def _get_table_column_value(self, obj, field):
        if not isinstance(field, ForeignKey):
            return getattr(obj, field.name)
        child_obj = getattr(obj, field.name)
        if not child_obj:
            return
        return getattr(child_obj, get_primary_key(child_obj.__class__).name)

    def _record_exists(self, obj):
        clz = obj.__class__
        self._check_table_exists_for_class(clz)
        primary_key = get_primary_key(clz)
        table_name = clz._table_name
        id_name = primary_key.name
        id_value = getattr(obj, id_name)
        query = f"""
            select count(*) from {table_name} where {id_name} = {id_value}
        """
        logging.debug(query)
        count = self._select_single(query)
        return count == 1

    @transactional
    def save_class(self, clz):
        """
        Create or replace class in database
        :param clz: class to save
        """
        self._save_class(clz)

    def _save_class(self, clz):
        self._check_is_table(clz)
        table_name = clz._table_name
        if table_name in self.db_tables:
            self._update_class(clz)
        else:
            self._create_class(clz)

    @transactional
    def save_hierarchy(self, root_class):
        """
        Create or replace class and child classes in database
        :param root_class: class to save
        """
        self._save_hierarchy(root_class)

    def _save_hierarchy(self, clz):
        self._check_is_table(clz)
        fields = get_class_database_fields(clz)
        refererenced_tables = list(filter(lambda field: isinstance(field, ForeignKey), fields))
        for refererenced_table in refererenced_tables:
            self._save_hierarchy(refererenced_table.mapping_class)
        self._save_class(clz)

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

    @transactional
    def delete_object(self, obj):
        """
        Delete object from database if it exists
        :param obj: object to delete
        """
        self._delete_object(obj)

    def _delete_object(self, obj):
        self._check_table_exists_for_class(obj.__class__)
        clz = obj.__class__
        primary_key = get_primary_key(clz)
        query = f"""
            delete from {clz._table_name} where id = {getattr(obj, primary_key.name)}
        """
        logging.debug(query)
        self._execute(query)

    @transactional
    def delete_class(self, clz):
        """
        Delete object from database if it exists else raise exception
        :param clz: class to delete
        """
        self._delete_class(clz)

    def _delete_class(self, clz):
        self._check_table_exists_for_class(clz)
        query = f"""
            drop table {clz._table_name}
        """
        logging.debug(query)
        self._execute(query)

    @transactional
    def delete_hierarchy(self, root_class):
        """
        Delete class and child classes from database if they exist else raise exception
        :param root_class: root class to start deletion
        """
        self._delete_hierarchy(root_class)

    def _delete_hierarchy(self, clz):
        self._check_table_exists_for_class(clz)
        fields = get_class_database_fields(clz)
        refererenced_tables = list(filter(lambda field: isinstance(field, ForeignKey), fields))
        self._delete_class(clz)
        for refererenced_table in refererenced_tables:
            self._delete_hierarchy(refererenced_table.mapping_class)

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

    def _format_field(self, value):
        if isinstance(value, str):
            return f"'{value}'"
        if value == None:
            return 'null'
        if JsonbField.is_type_supported(value):
            if isinstance(value, set) or isinstance(value, frozenset):
                value = list(value)
            if isinstance(value, ArrayType):
                value = value.tolist()
            return f"'{json.dumps(value)}'"
        return str(value)
