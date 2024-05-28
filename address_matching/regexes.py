from typing import Callable, List


def remove_commas_periods(input: str):
    return f"regexp_replace({input}, '[,.]', ' ', 'g')"


def remove_apostrophes(input: str):
    return f"regexp_replace({input}, e'\\'', '', 'g')"


def remove_multiple_spaces(input: str):
    return f"regexp_replace({input}, '\\s+', ' ', 'g')"


def standarise_num_dash_num(input: str):
    # Could consider the following if this is too aggressive:
    # '([A-Za-z0-9])\\s*-\\s*([A-Za-z0-9])', '\\1-\\2', 'g'
    return f"regexp_replace({input}, '\\s*\\-\\s*', '-', 'g')"


def replace_fwd_slash_with_dash(input: str):
    # Sometimes we see Unit 5/6 as opposed to unit 5-6
    return f"regexp_replace({input}, '/', '-', 'g')"


def remove_repeated_tokens(input: str):
    # If a token that's at least three characters long is repeated, remove the second instance
    return f"regexp_replace({input}, '\\b(\\w{3,})\\s+\\1\\b', '\\1', 'g')"


def trim(input: str):
    return f"trim({input})"


def move_flat_to_front(input: str):
    # Move reference to a 'flat + number' to the front of the string address string
    # e.g. FLAT 1
    # FLAT 1A

    return f"regexp_replace({input}, '^(FLAT \\d+[A-Z]?)\\s+(.*)$', '\\1 \\2', 'g')"


def construct_nested_call(col_name: str, fns: List[Callable]) -> str:
    input_str = col_name
    for f in fns:
        input_str = f(input_str)
    return input_str
