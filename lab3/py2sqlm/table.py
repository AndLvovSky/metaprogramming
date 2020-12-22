import inspect
from functools import wraps
from py2sqlm.utils import camel_case_to_snake_case


def table(param):
    """
    Decorator for tables.
    If table name is not specified it is a class name converted to snake case.
    :param param: either table name or class to decorate
    :return: either table wrapper or table class
    """
    if inspect.isclass(param):
        table_name = camel_case_to_snake_case(param.__name__)
        setattr(param, '_table_name', table_name)
        return param

    @wraps(param)
    def wrapper(clz):
        setattr(clz, '_table_name', param)
        return clz

    return wrapper
