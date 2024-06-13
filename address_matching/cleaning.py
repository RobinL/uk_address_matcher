import importlib.resources as pkg_resources

import duckdb
from duckdb import DuckDBPyRelation

from .regexes import (
    construct_nested_call,
    move_flat_to_front,
    remove_apostrophes,
    remove_commas_periods,
    remove_multiple_spaces,
    remove_repeated_tokens,
    replace_fwd_slash_with_dash,
    standarise_num_dash_num,
    standarise_num_letter,
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
            remove_apostrophes,
            remove_multiple_spaces,
            replace_fwd_slash_with_dash,
            standarise_num_dash_num,
            standarise_num_letter,
            move_flat_to_front,
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
        regexp_split_to_array(trim(address_without_numbers), '\\s+')
            AS address_without_numbers_tokenised
    from {table_name}
    """

    return duckdb.sql(sql)


def get_token_frequeny_table(table_name: str) -> DuckDBPyRelation:
    sql = f"""
    WITH concatenated_tokens AS (
        SELECT
            unique_id,
            list_concat(
                array_filter(
                    [numeric_token_1, numeric_token_2, numeric_token_3],
                    x -> x IS NOT NULL
                ),
                address_without_numbers_tokenised
            ) AS all_tokens
        FROM {table_name}
    ),
    unnested as (
    SELECT
            unnest(all_tokens) AS token
            FROM concatenated_tokens
    ),
    token_counts AS (
        SELECT
            token,
            count(*) AS count,
             count(*)  / (select count(*) from unnested) rel_freq
        from unnested
        GROUP BY token
    )
    SELECT
        token, count, rel_freq
    FROM token_counts
    ORDER BY count DESC
    """
    return duckdb.sql(sql)


def _tokens_with_freq_sql(
    table_name: str, rel_tok_freq_name: str = "rel_tok_freq"
) -> str:
    return f"""
     addresses_exploded AS (
        SELECT
            unique_id,
            unnest(address_without_numbers_tokenised) as token,
            generate_subscripts(address_without_numbers_tokenised, 1) AS token_order
        FROM {table_name}
    ),
    address_groups AS (
        SELECT addresses_exploded.*,
        COALESCE({rel_tok_freq_name}.rel_freq, 0.001) AS rel_freq
        FROM addresses_exploded
        LEFT JOIN {rel_tok_freq_name}
        ON addresses_exploded.token = {rel_tok_freq_name}.token
    ),
    token_freq_lookup AS (
        SELECT
            unique_id,
            -- This guarantees preservation of token order vs.
            -- list(struct_pack(token := token, rel_freq := rel_freq))
            list_transform(
                list_zip(
                    array_agg(token order by unique_id, token_order asc),
                    array_agg(rel_freq order by unique_id, token_order asc)
                    ),
                x-> struct_pack(tok:= x[1], rel_freq:= x[2])
            )
            as token_rel_freq_arr
        FROM address_groups
        GROUP BY unique_id
    )
    SELECT
        d.* EXCLUDE (address_without_numbers_tokenised),
        r.token_rel_freq_arr
    FROM
        {table_name} as d
    INNER JOIN token_freq_lookup as r
    ON d.unique_id = r.unique_id
    """


def add_term_frequencies_to_address_tokens(table_name: str) -> DuckDBPyRelation:
    # Compute relative term frequencies amongst the tokens
    sql = f"""
    WITH rel_tok_freq_cte AS (
        SELECT
            token,
            count(*)  / sum(count(*)) OVER() as rel_freq
        FROM (
            SELECT
                unnest(address_without_numbers_tokenised) as token
            FROM {table_name}
        )
        GROUP BY token
    ),
    {_tokens_with_freq_sql(table_name, rel_tok_freq_name="rel_tok_freq_cte")}
    """

    return duckdb.sql(sql)


def add_term_frequencies_to_address_tokens_using_registered_df(
    table_name: str,
) -> DuckDBPyRelation:

    sql = f"""
    WITH

    {_tokens_with_freq_sql(table_name)}
    """

    return duckdb.sql(sql)


def first_unusual_token(table_name: str) -> DuckDBPyRelation:

    # Get first below freq
    first_token = (
        "list_any_value(list_filter(token_rel_freq_arr, x -> x.rel_freq < 0.001))"
    )

    sql = f"""
    select
    {first_token} as first_unusual_token,
    *
    from {table_name}
    """
    return duckdb.sql(sql)


def use_first_unusual_token_if_no_numeric_token(table_name: str) -> DuckDBPyRelation:
    sql = f"""
    select
        * exclude (numeric_token_1, token_rel_freq_arr, first_unusual_token),
        case
            when numeric_token_1 is null then first_unusual_token.tok
            else numeric_token_1
            end as numeric_token_1,

    case
        when numeric_token_1 is null
        then list_filter(token_rel_freq_arr, x -> coalesce(x.tok != first_unusual_token.tok, true))
        else token_rel_freq_arr
    end
    as token_rel_freq_arr
    from {table_name}
    """

    return duckdb.sql(sql)


def final_column_order(table_name: str) -> DuckDBPyRelation:

    sql = f"""
    select
        unique_id,
        source_dataset,
        original_address_concat,
        numeric_token_1,
        numeric_token_2,
        numeric_token_3,
        list_transform(token_rel_freq_arr, x -> x.tok) as token_rel_freq_arr_readable,
        list_transform(common_end_tokens, x -> x.tok) as common_end_tokens_readable,
        token_rel_freq_arr,
        common_end_tokens,
        postcode,

    from {table_name}
    """

    return duckdb.sql(sql)


def move_common_end_tokens_to_field(table_name: str) -> DuckDBPyRelation:
    # Want to put common tokens towards the end of the address
    # into their own field.  These tokens (e.g. SOMERSET or LONDON)
    # are often ommitted from so 'punishing' lack of agreement is probably
    # not necessary

    with pkg_resources.path(
        "address_matching.data", "common_end_tokens.csv"
    ) as csv_path:

        sql = f"""
        select array_agg(token) as end_tokens_to_remove
        from read_csv_auto("{csv_path}")
        where token_count > 3000
        """
    common_end_tokens = duckdb.sql(sql)
    duckdb.register("common_end_tokens", common_end_tokens)

    end_tokens_as_array = """
    list_transform(common_end_tokens, x -> x.tok)
    """

    remove_end_tokens = f"""
    list_filter(token_rel_freq_arr,
        (x,i) ->
            not
            (
            i > len(token_rel_freq_arr) - 2
            and
            list_contains({end_tokens_as_array}, x.tok)
            )
    )
    """

    sql = f"""
    with

    joined as (
        select *
    from {table_name}
    cross join common_end_tokens
    ),

    end_tokens_included as (
    select
    * exclude (end_tokens_to_remove),
    original_address_concat,
    list_filter(token_rel_freq_arr[-3:],
        x ->  list_contains(end_tokens_to_remove, x.tok)
    )
    as common_end_tokens
    from joined
    )

    select
        * exclude (token_rel_freq_arr),
        {remove_end_tokens} as token_rel_freq_arr
    from end_tokens_included

    """

    return duckdb.sql(sql)
