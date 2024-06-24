import importlib.resources as pkg_resources

from duckdb import DuckDBPyConnection, DuckDBPyRelation

from uk_address_matcher.cleaning import (
    add_term_frequencies_to_address_tokens,
    add_term_frequencies_to_address_tokens_using_registered_df,
    clean_address_string_first_pass,
    clean_address_string_second_pass,
    derive_original_address_concat,
    extract_numeric_1_alt,
    final_column_order,
    first_unusual_token,
    move_common_end_tokens_to_field,
    parse_out_flat_positional,
    parse_out_numbers,
    separate_unusual_tokens,
    split_numeric_tokens_to_cols,
    tokenise_address_without_numbers,
    trim_whitespace_address_and_postcode,
    upper_case_address_and_postcode,
    use_first_unusual_token_if_no_numeric_token,
)
from uk_address_matcher.run_pipeline import run_pipeline


def clean_data_on_the_fly(
    address_table: DuckDBPyRelation,
    con: DuckDBPyConnection,
) -> DuckDBPyRelation:
    cleaning_queue = [
        trim_whitespace_address_and_postcode,
        upper_case_address_and_postcode,
        clean_address_string_first_pass,
        derive_original_address_concat,
        parse_out_flat_positional,
        extract_numeric_1_alt,
        parse_out_numbers,
        clean_address_string_second_pass,
        split_numeric_tokens_to_cols,
        tokenise_address_without_numbers,
        add_term_frequencies_to_address_tokens,
        move_common_end_tokens_to_field,
        first_unusual_token,
        use_first_unusual_token_if_no_numeric_token,
        separate_unusual_tokens,
        final_column_order,
    ]
    return run_pipeline(
        address_table, con=con, cleaning_queue=cleaning_queue, print_intermediate=False
    )


def clean_data_using_precomputed_rel_tok_freq(
    address_table: DuckDBPyRelation,
    con: DuckDBPyConnection,
    rel_tok_freq_table: DuckDBPyRelation = None,
) -> DuckDBPyRelation:

    # Load the default term frequency table if none is provided
    if rel_tok_freq_table is None:
        with pkg_resources.path(
            "uk_address_matcher.data", "address_token_frequencies.parquet"
        ) as default_tf_path:
            rel_tok_freq_table = con.read_parquet(str(default_tf_path))

    con.register("rel_tok_freq", rel_tok_freq_table)

    cleaning_queue = [
        trim_whitespace_address_and_postcode,
        upper_case_address_and_postcode,
        clean_address_string_first_pass,
        derive_original_address_concat,
        parse_out_flat_positional,
        extract_numeric_1_alt,
        parse_out_numbers,
        clean_address_string_second_pass,
        split_numeric_tokens_to_cols,
        tokenise_address_without_numbers,
        add_term_frequencies_to_address_tokens_using_registered_df,
        move_common_end_tokens_to_field,
        first_unusual_token,
        use_first_unusual_token_if_no_numeric_token,
        separate_unusual_tokens,
        final_column_order,
    ]

    return run_pipeline(
        address_table, con=con, cleaning_queue=cleaning_queue, print_intermediate=False
    )
