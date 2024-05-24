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


def split_numeric_tokens_to_cols(table_name: str) -> DuckDBPyRelation:
    sql = f"""
    select
        * exclude (numeric_tokens),
        numeric_tokens[1] as numeric_token_1,
        numeric_tokens[2] as numeric_token_2,
        numeric_tokens[3] as numeric_token_3
    from {table_name}
    """

    return duckdb.sql(sql)


def tokenise_address_without_numbers(table_name: str) -> DuckDBPyRelation:
    sql = f"""
    select
        * exclude (address_without_numbers),
        regexp_split_to_array(trim(address_without_numbers), '\\s')
            AS address_without_numbers_tokenised
    from {table_name}
    """

    return duckdb.sql(sql)


def add_term_frequencies_to_address_tokens(table_name: str) -> DuckDBPyRelation:
    # Compute relative term frequencies amongst the tokens
    sql = f"""
    WITH token_counts AS (
        SELECT
            token,
            count(*)  / sum(count(*)) OVER() as rel_freq
        FROM (
            SELECT
                unnest(address_without_numbers_tokenised) as token
            FROM {table_name}
        )
        GROUP BY token
        ORDER BY rel_freq DESC
    ),
    addresses_exploded AS (
        SELECT
            unique_id, unnest(address_without_numbers_tokenised) as token
        FROM {table_name}
    ),
    address_groups AS (
        SELECT addresses_exploded.*, token_counts.rel_freq
        FROM addresses_exploded
        LEFT JOIN token_counts
        ON addresses_exploded.token = token_counts.token
    ),
    token_freq_lookup AS (
    SELECT
        unique_id,
        -- list(struct_pack(token := token, rel_freq := rel_freq)) as token_rel_freq_arr
        list_zip(list(token), list(rel_freq))  as token_rel_freq_arr
    FROM address_groups
    GROUP BY unique_id)
    SELECT
        d.* exclude (address_without_numbers_tokenised), r.token_rel_freq_arr
    from
    {table_name} as d
    inner join token_freq_lookup as r
    on d.unique_id = r.unique_id

    """

    return duckdb.sql(sql)


def house_name_to_number_if_no_numbers(table_name: str) -> DuckDBPyRelation:

    # Get first below freq
    first_token = "list_filter(token_rel_freq_arr, x -> x.rel_freq < 0.01)"

    sql = f"""
    select
    list_any_value({first_token}) as first_token,
    *
    from {table_name}
    """

    # sql = f"""
    # select
    #     * exclude (numeric_token_1, address_without_numbers_tokenised),
    #     case
    #         when numeric_token_1 is null then address_without_numbers_tokenised[1]
    #         else numeric_token_1
    #         end as numeric_token_1,

    # case
    #     when numeric_token_1 is null then address_without_numbers_tokenised[2:]
    #     else address_without_numbers_tokenised
    # end
    # as address_without_numbers_tokenised
    # from {table_name}
    # """

    return duckdb.sql(sql)
