__version__ = "1.0.0.dev01"

from uk_address_matcher.linking_model.splink_model import get_linker
from uk_address_matcher.cleaning.cleaning_pipelines import (
    clean_data_on_the_fly,
    clean_data_using_precomputed_rel_tok_freq,
    get_numeric_term_frequencies_from_address_table,
    get_address_token_frequencies_from_address_table,
)

__all__ = [
    "get_linker",
    "clean_data_on_the_fly",
    "clean_data_using_precomputed_rel_tok_freq",
    "get_numeric_term_frequencies_from_address_table",
    "get_address_token_frequencies_from_address_table",
]
