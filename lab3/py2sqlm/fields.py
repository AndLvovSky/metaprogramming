import inspect
from array import ArrayType
from abc import ABCMeta, abstractmethod

class DataBaseField(metaclass=ABCMeta):
    def __init__(self, column_name=None, primary_key=False):
        self.column_name = column_name
        self.primary_key = primary_key

    def __set__(self, instance, value):
        if not self.is_valid_value(value):
            raise Exception(f'Value {value} for column {self.column_name} is invalid')
        instance.__dict__['_' + self.name] = value

    def __get__(self, instance, owner):
        return instance.__dict__['_' + self.name]

    def __set_name__(self, owner, name):
        self.name = name

    @property
    def column_name(self):
        if self._column_name:
            return self._column_name
        return self.name

    @column_name.setter
    def column_name(self, value):
        if not value is None and not isinstance(value, str):
            raise Exception(f'Column name {value} is invalid')
        self._column_name = value

    @property
    def primary_key(self):
        return self._primary_key

    @primary_key.setter
    def primary_key(self, value):
        if not value is None and not isinstance(value, bool):
            raise Exception(f'Primary key should have a boolean value')
        self._primary_key = value

    @property
    def definition(self):
        definition = f'{self.column_name} {self.column_type}'
        if self.primary_key:
            definition += ' primary key'
        return definition

    @property
    @abstractmethod
    def column_type(self):
        pass

    @abstractmethod
    def is_valid_value(self, value):
        pass

class IntField(DataBaseField):
    @property
    def column_type(self):
        return 'bigint'

    def is_valid_value(self, value):
        return isinstance(value, int)

class FloatField(DataBaseField):
    @property
    def column_type(self):
        return 'real'

    def is_valid_value(self, value):
        return isinstance(value, float) or isinstance(value, int)

class BoolField(DataBaseField):
    @property
    def column_type(self):
        return 'bool'

    def is_valid_value(self, value):
        return isinstance(value, bool)

class TextField(DataBaseField):
    MAX_TEXT_LENGTH = 10 ** 10

    def __init__(self, max_length=None, **kwargs):
        super().__init__(**kwargs)
        self.max_length = max_length

    @property
    def max_length(self):
        return self._max_length

    @max_length.setter
    def max_length(self, value):
        if value is None:
            self._max_length = self.MAX_TEXT_LENGTH
            return
        if not isinstance(value, int) or value < 1:
            raise Exception(f'Invalid max_length: {value}')
        self._max_length = value

    @property
    def column_type(self):
        if self.max_length == self.MAX_TEXT_LENGTH:
            return 'text'
        return f'varchar({self.max_length})'

    def is_valid_value(self, value):
        return isinstance(value, str) and len(value) < self.max_length

class JsonbField(DataBaseField):
    valid_types = {list, tuple, dict, set, frozenset, ArrayType}

    @staticmethod
    def is_type_supported(type):
        for valid_type in JsonbField.valid_types:
            if isinstance(type, valid_type):
                return True
        return False

    @property
    def column_type(self):
        return 'jsonb'

    def is_valid_value(self, value):
        return self.is_type_supported(value)

class ForeignKey(DataBaseField):
    def __init__(self, mapping_class, mapping_column=None, **kwargs):
        super().__init__(**kwargs)
        self.mapping_class = mapping_class
        if mapping_column:
            self.mapping_column = mapping_column
        else:
            self.mapping_column = self.mapping_class._table_name + '_id'

    @property
    def mapping_class(self):
        return self._mapping_class

    @mapping_class.setter
    def mapping_class(self, value):
        if not inspect.isclass(value):
            raise Exception(f'Invalid mapping class: {value}')
        self._mapping_class = value

    @property
    def mapping_column(self):
        return self._mapping_column

    @mapping_column.setter
    def mapping_column(self, value):
        if not isinstance(value, str):
            raise Exception(f'Invalid mapping column: {value}')
        self._mapping_column = value

    @property
    def column_type(self):
        primary_key = get_primary_key(self.mapping_class)
        return primary_key.column_type

    def is_valid_value(self, value):
        return isinstance(value, self.mapping_class)

    @property
    def definition(self):
        definition = super().definition
        definition += f' references {self.mapping_class._table_name}' \
            f' ({get_primary_key(self.mapping_class).name})'
        return definition

def get_class_database_fields(clz):
    fields = list(filter(lambda value: isinstance(value, DataBaseField), clz.__dict__.values()))
    if len(fields) < 1:
        raise Exception('Table should have at least one column')
    return fields

def get_primary_key(clz):
    fields = get_class_database_fields(clz)
    primary_keys = list(filter(lambda field: field.primary_key, fields))
    if len(primary_keys) != 1:
        raise Exception('Table should have exactly one primary key')
    return primary_keys[0]

class ManyRelation:
    def __init__(self, mapping_class):
        self.mapping_class = mapping_class

    def __set__(self, instance, value):
        if not value:
            value = []
        if not isinstance(value, list) or not self._is_all_mapping_objects(value):
            raise Exception(f'{self.name} should be a list of {self.mapping_class.__name__}')
        instance.__dict__['_' + self.name] = value

    def __get__(self, instance, owner):
        return instance.__dict__['_' + self.name]

    def __set_name__(self, owner, name):
        self.name = name

    @property
    def mapping_class(self):
        return self._mapping_class

    @mapping_class.setter
    def mapping_class(self, value):
        if not inspect.isclass(value):
            raise Exception(f'Invalid mapping class: {value}')
        self._mapping_class = value

    def _is_all_mapping_objects(self, obj_list):
        if not obj_list:
            return True
        for obj in obj_list:
            if not isinstance(obj, self.mapping_class):
                return False
        return True


def get_class_database_fields(clz):
    fields = list(filter(_is_database_field, clz.__dict__.values()))
    if len(fields) < 1:
        raise Exception('Table should have at least one column')
    return fields

def _is_database_field(field):
    return isinstance(field, DataBaseField) or isinstance(field, ForeignKey)

def get_primary_key(clz):
    fields = get_class_database_fields(clz)
    primary_keys = list(filter(lambda field: hasattr(field, 'primary_key') and field.primary_key, fields))
    if len(primary_keys) != 1:
        raise Exception('Table should have exactly one primary key')
    return primary_keys[0]
