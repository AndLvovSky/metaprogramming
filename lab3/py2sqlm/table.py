import inspect
from functools import wraps
from py2sqlm.utils import camel_case_to_snake_case

def table(param):
    if inspect.isclass(param):
        table_name = camel_case_to_snake_case(param.__name__)
        setattr(param, '_table_name', table_name)
        return param

    @wraps(param)
    def wrapper(clz):
        setattr(clz, '_table_name', param)
        return clz

    return wrapper
