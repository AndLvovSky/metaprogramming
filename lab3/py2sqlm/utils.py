import re

def camel_case_to_snake_case(text):
    """
    Convert text from camel case to snake case
    :param text: camel case text
    :return: snake case text
    """
    return re.sub(r'(?<!^)(?=[A-Z])', '_', text).lower()
