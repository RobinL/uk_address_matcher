import duckdb
from duckdb import DuckDBPyRelation

from address_matching.cleaning import (
    add_term_frequencies_to_address_tokens,
    add_term_frequencies_to_address_tokens_using_registered_df,
    clean_address_string_first_pass,
    clean_address_string_second_pass,
    final_column_order,
    first_unusual_token,
    move_common_end_tokens_to_field,
    parse_out_numbers,
    split_numeric_tokens_to_cols,
    tokenise_address_without_numbers,
    trim_whitespace_address_and_postcode,
    upper_case_address_and_postcode,
    use_first_unusual_token_if_no_numeric_token,
)
from address_matching.run_pipeline import run_pipeline


def clean_data_on_the_fly(address_table: DuckDBPyRelation) -> DuckDBPyRelation:
    cleaning_queue = [
        trim_whitespace_address_and_postcode,
        upper_case_address_and_postcode,
        clean_address_string_first_pass,
        parse_out_numbers,
        clean_address_string_second_pass,
        split_numeric_tokens_to_cols,
        tokenise_address_without_numbers,
        add_term_frequencies_to_address_tokens,
        move_common_end_tokens_to_field,
        first_unusual_token,
        use_first_unusual_token_if_no_numeric_token,
        final_column_order,
    ]
    run_pipeline(address_table, cleaning_queue, print_intermediate=False)


def clean_data_using_precomputed_rel_tok_freq(
    address_table: DuckDBPyRelation,
    rel_tok_freq_table: DuckDBPyRelation,
) -> DuckDBPyRelation:

    duckdb.register("rel_tok_freq", rel_tok_freq_table)

    cleaning_queue = [
        trim_whitespace_address_and_postcode,
        upper_case_address_and_postcode,
        clean_address_string_first_pass,
        parse_out_numbers,
        clean_address_string_second_pass,
        split_numeric_tokens_to_cols,
        tokenise_address_without_numbers,
        add_term_frequencies_to_address_tokens_using_registered_df,
        move_common_end_tokens_to_field,
        first_unusual_token,
        use_first_unusual_token_if_no_numeric_token,
        final_column_order,
    ]

    return run_pipeline(address_table, cleaning_queue, print_intermediate=False)
