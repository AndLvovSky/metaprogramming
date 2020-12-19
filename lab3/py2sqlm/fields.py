import inspect
from array import ArrayType
from abc import ABCMeta, abstractmethod

class DatabaseField(metaclass=ABCMeta):
    """
    Database field descriptor
    """

    def __init__(self, column_name=None, primary_key=False):
        """
        Construct database field
        :param column_name: table column name
        :param primary_key: is column a primary key
        """
        self.column_name = column_name
        self.primary_key = primary_key

    def __set__(self, instance, value):
        """
        Validate and set database field using is_valid_value abstract method
        """
        if not self.is_valid_value(value):
            raise Exception(f'Value {value} for column {self.column_name} is invalid')
        instance.__dict__['_' + self.name] = value

    def __get__(self, instance, owner):
        """
        Return database field value
        """
        return instance.__dict__['_' + self.name]

    def __set_name__(self, owner, name):
        """
        Set attribute name
        """
        self.name = name

    @property
    def column_name(self):
        """
        Table column name property
        """
        if self._column_name:
            return self._column_name
        return self.name

    @column_name.setter
    def column_name(self, value):
        """
        Set table column name
        """
        if not value is None and not isinstance(value, str):
            raise Exception(f'Column name {value} is invalid')
        self._column_name = value

    @property
    def primary_key(self):
        """
        Return True if column is a primary key
        """
        return self._primary_key

    @primary_key.setter
    def primary_key(self, value):
        """
        Set primary key flag
        """
        if not value is None and not isinstance(value, bool):
            raise Exception(f'Primary key should have a boolean value')
        self._primary_key = value

    @property
    def definition(self):
        """
        SQL type definition property
        """
        definition = f'{self.column_name} {self.column_type}'
        if self.primary_key:
            definition += ' primary key'
        return definition

    @property
    @abstractmethod
    def column_type(self):
        """
        Return SQL column type string
        """
        pass

    @abstractmethod
    def is_valid_value(self, value):
        """
        Return True if value is a valid table value
        """
        pass

class IntField(DatabaseField):
    """
    Integer database field descriptor
    """

    @property
    def column_type(self):
        return 'bigint'

    def is_valid_value(self, value):
        return isinstance(value, int)

class FloatField(DatabaseField):
    """
    Float database field descriptor
    """

    @property
    def column_type(self):
        return 'real'

    def is_valid_value(self, value):
        return isinstance(value, float) or isinstance(value, int)

class BoolField(DatabaseField):
    """
    Boolean database field descriptor
    """

    @property
    def column_type(self):
        return 'bool'

    def is_valid_value(self, value):
        return isinstance(value, bool)

class TextField(DatabaseField):
    """
    Text database field descriptor.
    Maximum text length can be specified via max_length parameter
    """

    _MAX_TEXT_LENGTH = 10 ** 10

    def __init__(self, max_length=None, **kwargs):
        """
        Construct text database field
        :param max_length: maximum text length
        :param kwargs: database field parameters
        """
        super().__init__(**kwargs)
        self.max_length = max_length

    @property
    def max_length(self):
        """
        Max text length property
        """
        return self._max_length

    @max_length.setter
    def max_length(self, value):
        """
        Set max text length
        """
        if value is None:
            self._max_length = self._MAX_TEXT_LENGTH
            return
        if not isinstance(value, int) or value < 1:
            raise Exception(f'Invalid max_length: {value}')
        self._max_length = value

    @property
    def column_type(self):
        if self.max_length == self._MAX_TEXT_LENGTH:
            return 'text'
        return f'varchar({self.max_length})'

    def is_valid_value(self, value):
        return isinstance(value, str) and len(value) < self.max_length

class JsonbField(DatabaseField):
    """
    Jsonb database field descriptor.
    Following types are allowed: list, tuple, dict, set, frozenset, array
    """

    valid_types = {list, tuple, dict, set, frozenset, ArrayType}

    @staticmethod
    def is_type_supported(type):
        """
        :return: True if specified type is supported
        """
        for valid_type in JsonbField.valid_types:
            if isinstance(type, valid_type):
                return True
        return False

    @property
    def column_type(self):
        return 'jsonb'

    def is_valid_value(self, value):
        return self.is_type_supported(value)

class ForeignKey(DatabaseField):
    """
    Foreign key descriptor.
    Maps attrribute to object of specified class
    """

    def __init__(self, mapping_class, mapping_column=None, **kwargs):
        """
        Construct foreign key
        :param mapping_class: object to map class
        :param mapping_column: mapping column name
        (default - mapping class name converted to snake case plus '_id')
        :param kwargs: database field parameters
        """
        super().__init__(**kwargs)
        self.mapping_class = mapping_class
        if mapping_column:
            self.mapping_column = mapping_column
        else:
            self.mapping_column = self.mapping_class._table_name + '_id'

    @property
    def mapping_class(self):
        """
        Mapping class property
        """
        return self._mapping_class

    @mapping_class.setter
    def mapping_class(self, value):
        """
        Set mapping class
        """
        if not inspect.isclass(value):
            raise Exception(f'Invalid mapping class: {value}')
        self._mapping_class = value

    @property
    def mapping_column(self):
        """
        Mapping column property
        """
        return self._mapping_column

    @mapping_column.setter
    def mapping_column(self, value):
        """
        Set mapping column
        """
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
        """
        Overrides DatabaseField definition
        """
        definition = f'{self.column_name}_id {self.column_type}'
        definition += f' references {self.mapping_class._table_name}' \
            f' ({get_primary_key(self.mapping_class).name})'
        return definition

class ManyRelation:
    """
    Many relation descriptor.
    Used to map one-to-many or many-to-many relations
    """
    def __init__(self, mapping_class):
        """
        Construct many relation descriptor
        :param mapping_class: objects to map class
        """
        self.mapping_class = mapping_class

    def __set__(self, instance, value):
        """
        Validate and set list of referencing objects
        """
        if not value:
            value = []
        if not isinstance(value, list) or not self._is_all_mapping_objects(value):
            raise Exception(f'{self.name} should be a list of {self.mapping_class.__name__}')
        instance.__dict__['_' + self.name] = value

    def __get__(self, instance, owner):
        """
        Return list of referencing objects
        """
        return instance.__dict__['_' + self.name]

    def __set_name__(self, owner, name):
        """
        Set attribute name
        """
        self.name = name

    @property
    def mapping_class(self):
        """
        Mapping class property
        """
        return self._mapping_class

    @mapping_class.setter
    def mapping_class(self, value):
        """
        Set mapping class
        """
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
    """
    Helper method to get all database fields for class.
    :param clz: table class
    :return: list of database fields
    """
    fields = list(filter(_is_database_field, clz.__dict__.values()))
    if len(fields) < 1:
        raise Exception('Table should have at least one column')
    return fields

def _is_database_field(field):
    return isinstance(field, DatabaseField) or isinstance(field, ForeignKey)

def get_primary_key(clz):
    """
    Helper method to return class primary key.
    Raise exception if none is found
    :param clz: table class
    :return: primary key field
    """
    fields = get_class_database_fields(clz)
    primary_keys = list(filter(lambda field: hasattr(field, 'primary_key') and field.primary_key, fields))
    if len(primary_keys) != 1:
        raise Exception('Table should have exactly one primary key')
    return primary_keys[0]
