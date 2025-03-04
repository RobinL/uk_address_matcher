from importlib import resources
import random
import string

from duckdb import DuckDBPyConnection, DuckDBPyRelation

from uk_address_matcher.cleaning.cleaning_steps import (
    add_term_frequencies_to_address_tokens,
    add_term_frequencies_to_address_tokens_using_registered_df,
    clean_address_string_first_pass,
    clean_address_string_second_pass,
    derive_original_address_concat,
    final_column_order,
    first_unusual_token,
    move_common_end_tokens_to_field,
    parse_out_flat_position_and_letter,
    parse_out_numbers,
    separate_unusual_tokens,
    split_numeric_tokens_to_cols,
    tokenise_address_without_numbers,
    trim_whitespace_address_and_postcode,
    upper_case_address_and_postcode,
    use_first_unusual_token_if_no_numeric_token,
    get_token_frequeny_table,
)
from uk_address_matcher.cleaning.run_pipeline import run_pipeline


def _generate_random_identifier(length=8):
    characters = string.ascii_letters + string.digits
    return "".join(random.choice(characters) for _ in range(length))


QUEUE_PRE_TF = [
    trim_whitespace_address_and_postcode,
    upper_case_address_and_postcode,
    clean_address_string_first_pass,
    derive_original_address_concat,
    parse_out_flat_position_and_letter,
    parse_out_numbers,
    clean_address_string_second_pass,
    split_numeric_tokens_to_cols,
    tokenise_address_without_numbers,
]


QUEUE_POST_TF = [
    move_common_end_tokens_to_field,
    first_unusual_token,
    use_first_unusual_token_if_no_numeric_token,
    separate_unusual_tokens,
    final_column_order,
]


def clean_data_on_the_fly(
    address_table: DuckDBPyRelation,
    con: DuckDBPyConnection,
) -> DuckDBPyRelation:
    cleaning_queue = (
        QUEUE_PRE_TF
        + [
            add_term_frequencies_to_address_tokens,
        ]
        + QUEUE_POST_TF
    )

    # If the following create temp table is not included
    # and `address_table` is created from like
    # select * from read_parquet() order by random()
    # the rest does not work

    uid = _generate_random_identifier()
    con.register("__address_table_in", address_table)

    materialised_table_name = f"__address_table_{uid}"
    sql = f"""
    create or replace temporary table {materialised_table_name} as
    select * from __address_table_in
    """
    con.execute(sql)
    input_table = con.table(materialised_table_name)

    res = run_pipeline(input_table, con=con, cleaning_queue=cleaning_queue)

    materialised_cleaned_table_name = f"__address_table_cleaned_{uid}"
    con.register("__address_table_res", res)

    # Check if source_dataset column exists and exclude it if it does
    has_source_dataset = "source_dataset" in res.columns

    exclude_clause = "EXCLUDE (source_dataset)" if has_source_dataset else ""

    sql = f"""
    create or replace temporary table {materialised_cleaned_table_name} as
    select * {exclude_clause} from __address_table_res
    """
    con.execute(sql)
    return con.table(materialised_cleaned_table_name)


def clean_data_using_precomputed_rel_tok_freq(
    address_table: DuckDBPyRelation,
    con: DuckDBPyConnection,
    rel_tok_freq_table: DuckDBPyRelation = None,
) -> DuckDBPyRelation:
    # Load the default term frequency table if none is provided
    if rel_tok_freq_table is None:
        default_tf_path = (
            resources.files("uk_address_matcher")
            / "data"
            / "address_token_frequencies.parquet"
        )
        rel_tok_freq_table = con.read_parquet(str(default_tf_path))

    con.register("rel_tok_freq", rel_tok_freq_table)

    # If the following create temp table is not included
    # and `address_table` is created from like
    # select * from read_parquet() order by random()
    # the rest does not work

    uid = _generate_random_identifier()

    con.register("__address_table_in", address_table)

    materialised_table_name = f"__address_table_{uid}"
    sql = f"""
    create or replace temporary table {materialised_table_name} as
    select * from __address_table_in
    """
    con.execute(sql)
    input_table = con.table(materialised_table_name)

    cleaning_queue = (
        QUEUE_PRE_TF
        + [
            add_term_frequencies_to_address_tokens_using_registered_df,
        ]
        + QUEUE_POST_TF
    )

    res = run_pipeline(
        input_table,
        con=con,
        cleaning_queue=cleaning_queue,
    )

    materialised_cleaned_table_name = f"__address_table_cleaned_{uid}"
    con.register("__address_table_res", res)

    # Check if source_dataset column exists and exclude it if it does
    has_source_dataset = "source_dataset" in res.columns

    exclude_clause = "EXCLUDE (source_dataset)" if has_source_dataset else ""

    sql = f"""
    create or replace temporary table {materialised_cleaned_table_name} as
    select * {exclude_clause} from __address_table_res
    """
    con.execute(sql)
    return con.table(materialised_cleaned_table_name)


def get_numeric_term_frequencies_from_address_table(
    df_address_table: DuckDBPyRelation,
    con: DuckDBPyConnection,
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
    df_address_table: DuckDBPyRelation, con: DuckDBPyConnection
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
