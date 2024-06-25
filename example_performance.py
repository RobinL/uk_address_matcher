import duckdb
import pandas as pd
from IPython.display import display

from uk_address_matcher.cleaning_pipelines import (
    clean_data_using_precomputed_rel_tok_freq,
)
from uk_address_matcher.splink_model import _performance_predict

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

# Remove the limit statements to run the full dataset, the limit is just to speed up
# the example
p_ch = "./example_data/companies_house_addresess_postcode_overlap.parquet"
p_fhrs = "./example_data/fhrs_addresses_sample.parquet"
con = duckdb.connect(database=":memory:")
df_1 = con.read_parquet(p_ch).order("postcode")
df_2 = con.read_parquet(p_fhrs).order("postcode")


# -----------------------------------------------------------------------------
# Step 2: Clean the data/feature engineering to prepare for matching model
# -----------------------------------------------------------------------------


# See notes at the end re:using precomputed term frequencies
df_1_c = clean_data_using_precomputed_rel_tok_freq(df_1, con=con)
df_2_c = clean_data_using_precomputed_rel_tok_freq(df_2, con=con)


linker, predictions = _performance_predict(
    [df_1_c, df_2_c],
    con=con,
    match_weight_threshold=-10,
    output_all_cols=True,
    include_full_postcode_block=True,
)

# ------------------------------------------------------------------------------------
# Step 3: Inspect the results:
# ------------------------------------------------------------------------------------

sql = """
SELECT
    match_probability,
    match_weight,
    concat_ws(' ', original_address_concat_l, postcode_l) AS address_l,
    concat_ws(' ', original_address_concat_r, postcode_r) AS address_r,
    unique_id_l,
    unique_id_r,
    source_dataset_l,
    source_dataset_r
FROM
    predictions
WHERE
    match_weight > 0
QUALIFY
    row_number() OVER (
        PARTITION BY unique_id_l ORDER BY match_weight DESC
    ) = 1

"""

top_predict = con.sql(sql).df()

display(top_predict.head())


sql = """
SELECT * FROM predictions WHERE match_weight > 0
QUALIFY row_number() OVER (PARTITION BY unique_id_l ORDER BY match_weight DESC) = 1
order by random()
limit 3
"""

recs = con.sql(sql).df().to_dict(orient="records")


for rec in recs:
    print("-" * 80)
    print(rec["unique_id_l"], rec["original_address_concat_l"])
    print(rec["unique_id_r"], rec["original_address_concat_r"])
    display(linker.waterfall_chart([rec]))
