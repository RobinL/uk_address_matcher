__version__ = "1.0.0.dev15"

from uk_address_matcher.linking_model.splink_model import get_linker
from uk_address_matcher.cleaning.cleaning_pipelines import (
    clean_data_on_the_fly,
    clean_data_using_precomputed_rel_tok_freq,
    get_numeric_term_frequencies_from_address_table,
    get_address_token_frequencies_from_address_table,
)
from uk_address_matcher.post_linkage.identify_distinguishing_tokens import (
    improve_predictions_using_distinguishing_tokens,
)

from uk_address_matcher.post_linkage.analyse_results import (
    best_matches_with_distinguishability,
    best_matches_summary,
)

__all__ = [
    "get_linker",
    "clean_data_on_the_fly",
    "clean_data_using_precomputed_rel_tok_freq",
    "get_numeric_term_frequencies_from_address_table",
    "get_address_token_frequencies_from_address_table",
    "improve_predictions_using_distinguishing_tokens",
    "best_matches_with_distinguishability",
    "best_matches_summary",
]
