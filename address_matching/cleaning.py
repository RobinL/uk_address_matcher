import duckdb
from duckdb import DuckDBPyRelation

from .regexes import (
    construct_nested_call,
    remove_commas_periods,
    remove_multiple_spaces,
    remove_repeated_tokens,
    replace_fwd_slash_with_dash,
    standarise_num_dash_num,
    trim,
)


def upper_case_address_and_postcode(table_name: str) -> DuckDBPyRelation:
    sql = f"""
    select
        * exclude (address_concat, postcode),
        upper(address_concat) as address_concat,
        upper(postcode) as postcode
    from {table_name}
    """

    return duckdb.sql(sql)


def trim_whitespace_address_and_postcode(table_name: str) -> DuckDBPyRelation:
    sql = f"""
    select
        * exclude (address_concat, postcode),
        trim(address_concat) as address_concat,
        trim(postcode) as postcode
    from {table_name}
    """

    return duckdb.sql(sql)


def clean_address_string_first_pass(table_name: str) -> DuckDBPyRelation:
    fn_call = construct_nested_call(
        "address_concat",
        [
            remove_commas_periods,
            remove_multiple_spaces,
            replace_fwd_slash_with_dash,
            standarise_num_dash_num,
            remove_repeated_tokens,
            trim,
        ],
    )
    sql = f"""
    select
        * exclude (address_concat),
        {fn_call} as address_concat,

    from {table_name}
    """

    return duckdb.sql(sql)


def parse_out_numbers(table_name: str) -> DuckDBPyRelation:
    # Pattern to detect tokens with numbers in them inc e.g. 10A-20

    regex_pattern = r"\b(?:\d*[\w\-]*\d+[\w\-]*|\w(?:-\w)?)\b"
    sql = f"""
    SELECT
        * exclude (address_concat),
        regexp_replace(address_concat, '{regex_pattern}', '', 'g')
            AS address_without_numbers,
        regexp_extract_all(address_concat, '{regex_pattern}')
            AS numeric_tokens
        from {table_name}
        """
    return duckdb.sql(sql)


def clean_address_string_second_pass(table_name: str) -> DuckDBPyRelation:
    fn_call = construct_nested_call(
        "address_without_numbers",
        [remove_multiple_spaces, trim],
    )
    sql = f"""
    select
        * exclude (address_without_numbers),
        {fn_call} as address_without_numbers,

    from {table_name}
    """

    return duckdb.sql(sql)
