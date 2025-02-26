from tests.utils import prepare_combined_test_data
from uk_address_matcher import clean_data_using_precomputed_rel_tok_freq, get_linker
from uk_address_matcher.post_linkage.identify_distinguishing_tokens import (
    improve_predictions_using_distinguishing_tokens,
)
import duckdb
from splink import block_on
from IPython.display import display

MATCH_WEIGHT_THRESHOLD_PREDICT = -50
MATCH_WEIGHT_THRESHOLD_IMPROVE = -20

duckdb_con = duckdb.connect(database=":memory:")
yaml_path = "tests/test_addresses.yaml"

# Prepare data
messy_addresses, canonical_addresses = prepare_combined_test_data(yaml_path, duckdb_con)

test_block = 7
messy_addresses = messy_addresses.filter(f"test_block = {test_block}")
canonical_addresses = canonical_addresses.filter(f"test_block = {test_block}")

# Clean the input data
messy_clean = clean_data_using_precomputed_rel_tok_freq(messy_addresses, con=duckdb_con)
canonical_clean = clean_data_using_precomputed_rel_tok_freq(
    canonical_addresses, con=duckdb_con
)

# Configure the linker
columns_to_retain = ["original_address_concat", "true_match_id"]
linker = get_linker(
    df_addresses_to_match=messy_clean,
    df_addresses_to_search_within=canonical_clean,
    con=duckdb_con,
    include_full_postcode_block=True,
    additional_columns_to_retain=columns_to_retain,
)
linker._settings_obj._blocking_rules_to_generate_predictions = [
    block_on("test_block").get_blocking_rule("duckdb")
]

# Predict matches (first pass)
predicted_matches = linker.inference.predict(
    threshold_match_weight=MATCH_WEIGHT_THRESHOLD_PREDICT,
    experimental_optimisation=True,
).as_duckdbpyrelation()
predicted_matches
recs = predicted_matches.filter("unique_id_l = '7001'").df().to_dict(orient="records")
display(linker.visualisations.waterfall_chart(recs, filter_nulls=False))

recs = predicted_matches.filter("unique_id_l = '7001'")


# Extract left and right records from a single match
sql = """
WITH df AS (
    SELECT *
    FROM recs

)
SELECT COLUMNS('^(t[^f_].*|b[^f_].*|[^tb].*)_l$') AS '\\1'
FROM df
UNION ALL
SELECT COLUMNS('^(t[^f_].*|b[^f_].*|[^tb].*)_r$') AS '\\1'
FROM df;
"""

duckdb_con.sql(sql).show(max_width=10000)

predicted_matches.show(max_width=10000)
# Improve predictions (second pass)
improved_matches = improve_predictions_using_distinguishing_tokens(
    df_predict=predicted_matches,
    con=duckdb_con,
    match_weight_threshold=MATCH_WEIGHT_THRESHOLD_IMPROVE,
)
improved_matches.show(max_width=10000)
