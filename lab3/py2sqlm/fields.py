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
        return isinstance(value, float)

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
