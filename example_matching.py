import duckdb
import pandas as pd

from uk_address_matcher.post_linkage.analyse_results import (
    distinguishability_summary,
)
from uk_address_matcher.post_linkage.identify_distinguishing_tokens import (
    improve_predictions_using_distinguishing_tokens,
)
from uk_address_matcher import clean_data_using_precomputed_rel_tok_freq, get_linker
import time

pd.options.display.max_colwidth = 1000

# -----------------------------------------------------------------------------
# Step 1: Load in some example data
# -----------------------------------------------------------------------------

# If you're using your own data you need the following columns:
#
# +-------------------+--------------------------------------------------------+
# |      Column       |                     Description                        |
# +-------------------+--------------------------------------------------------+
# | unique_id         | Unique identifier for each record                      |
# |                   |                                                        |
# | source_dataset    | Populated with a constant string identifying the       |
# |                   | dataset, e.g. 'epc'                                    |
# |                   |                                                        |
# | address_concat    | Full address concatenated as without  postcode         |
# |                   |                                                        |
# | postcode          | Postcode                                               |
# +-------------------+--------------------------------------------------------+


# Any additional columns should be retained as-is by the cleaning code

p_ch = "./example_data/companies_house_addresess_postcode_overlap.parquet"
p_fhrs = "./example_data/fhrs_addresses_sample.parquet"

con = duckdb.connect(database=":memory:")

df_ch = con.read_parquet(p_ch).order("postcode")
df_fhrs = con.read_parquet(p_fhrs).order("postcode")

# -----------------------------------------------------------------------------
# Step 2: Clean the data/feature engineering to prepare for matching model
# -----------------------------------------------------------------------------

df_ch_clean = clean_data_using_precomputed_rel_tok_freq(df_ch, con=con)
df_fhrs_clean = clean_data_using_precomputed_rel_tok_freq(df_fhrs, con=con)

# -----------------------------------------------------------------------------
# Step 3: Link the data using Splink
# -----------------------------------------------------------------------------


linker = get_linker(
    df_addresses_to_match=df_fhrs_clean,
    df_addresses_to_search_within=df_ch_clean,
    con=con,
    include_full_postcode_block=True,
    additional_columns_to_retain=["original_address_concat"],
)

df_predict = linker.inference.predict(
    threshold_match_weight=-50, experimental_optimisation=True
)
df_predict_ddb = df_predict.as_duckdbpyrelation()

# -----------------------------------------------------------------------------
# Step 4: There's an optimisation we can do post-linking to improve score
# described here https://github.com/RobinL/uk_address_matcher/issues/14
# -----------------------------------------------------------------------------

start_time = time.time()
df_predict_improved = improve_predictions_using_distinguishing_tokens(
    df_predict=df_predict_ddb,
    con=con,
    match_weight_threshold=-20,
)

df_predict_improved.show(max_width=500, max_rows=20)

end_time = time.time()
print(f"Time taken: {end_time - start_time} seconds")
# -----------------------------------------------------------------------------

# # -----------------------------------------------------------------------------
# # Step 4: Get summary results of the match accuracy by taking the best match
# # for each FHRS address
# # -----------------------------------------------------------------------------

dsum_1 = distinguishability_summary(
    df_predict=df_predict_ddb, df_addresses_to_match=df_fhrs_clean, con=con
)
dsum_1.show(max_width=500, max_rows=20)

dsum_2 = distinguishability_summary(
    df_predict=df_predict_improved, df_addresses_to_match=df_fhrs_clean, con=con
)
dsum_2.show(max_width=500, max_rows=20)
