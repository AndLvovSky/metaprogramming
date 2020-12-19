import re

def camel_case_to_snake_case(text):
    return re.sub(r'(?<!^)(?=[A-Z])', '_', text).lower()
