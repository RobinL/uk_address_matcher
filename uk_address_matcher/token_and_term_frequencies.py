import duckdb
from duckdb import DuckDBPyConnection, DuckDBPyRelation

from uk_address_matcher.cleaning import (
    clean_address_string_first_pass,
    clean_address_string_second_pass,
    get_token_frequeny_table,
    parse_out_numbers,
    split_numeric_tokens_to_cols,
    tokenise_address_without_numbers,
    trim_whitespace_address_and_postcode,
    upper_case_address_and_postcode,
)
from uk_address_matcher.run_pipeline import run_pipeline


def get_numeric_term_frequencies_from_address_table(
    df_address_table: DuckDBPyRelation,
    con: duckdb.DuckDBPyConnection,
) -> DuckDBPyRelation:
    cleaning_queue = [
        trim_whitespace_address_and_postcode,
        upper_case_address_and_postcode,
        clean_address_string_first_pass,
        parse_out_numbers,
    ]

    numeric_tokens_df = run_pipeline(
        df_address_table,
        cleaning_queue=cleaning_queue,
        print_intermediate=False,
        con=con,
    )
    con.register("numeric_tokens_df", numeric_tokens_df)

    # For splink, table needs to be in the format
    # numeric_token__2, tf_numeric_token_2_l
    sql = """
    with unnested as
    (select unnest(numeric_tokens)  as numeric_token
    from numeric_tokens_df)

    select
    numeric_token,
    count(*)/(select count(*) from unnested) as tf_numeric_token
    from  unnested
    group by numeric_token
    order by 2 desc

    """
    return con.sql(sql)


def get_address_token_frequencies_from_address_table(
    df_address_table: DuckDBPyRelation, con: duckdb.DuckDBPyConnection
):
    cleaning_queue = [
        trim_whitespace_address_and_postcode,
        upper_case_address_and_postcode,
        clean_address_string_first_pass,
        parse_out_numbers,
        clean_address_string_second_pass,
        split_numeric_tokens_to_cols,
        tokenise_address_without_numbers,
        get_token_frequeny_table,
    ]

    return run_pipeline(
        df_address_table,
        cleaning_queue=cleaning_queue,
        print_intermediate=False,
        con=con,
    )
