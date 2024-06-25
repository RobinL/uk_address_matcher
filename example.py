import duckdb
from IPython.display import display

from uk_address_matcher.cleaning_pipelines import (
    clean_data_using_precomputed_rel_tok_freq,
)
from uk_address_matcher.display_results import distinguishability
from uk_address_matcher.splink_model import get_pretrained_linker

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
con = duckdb.connect(database=":memory:")
p_ch = "./example_data/companies_house_addresess_postcode_overlap.parquet"
p_fhrs = "./example_data/fhrs_addresses_sample.parquet"
con = duckdb.connect(database=":memory:")
df_1 = con.read_parquet(p_ch).order("postcode").limit(100)
df_2 = con.read_parquet(p_fhrs).order("postcode").limit(100)


# -----------------------------------------------------------------------------
# Step 2: Clean the data/feature engineering to prepare for matching model
# -----------------------------------------------------------------------------


# See notes at the end re:using precomputed term frequencies
df_1_c = clean_data_using_precomputed_rel_tok_freq(df_1, con=con)
df_2_c = clean_data_using_precomputed_rel_tok_freq(df_2, con=con)


# All dfs going in here are of type DuckDBPyRelation
# See note at end re: using precomputed term frequencies
linker = get_pretrained_linker(
    [df_1_c, df_2_c], con=con, include_full_postcode_block=True, salting_multiplier=2
)
linker.cumulative_num_comparisons_from_blocking_rules_chart()

df_predict = linker.predict(threshold_match_weight=-10)


# ------------------------------------------------------------------------------------
# Step 3: Inspect the results:
# ------------------------------------------------------------------------------------

sql = f"""
select
    unique_id_l,
    unique_id_r,
    source_dataset_l,
    source_dataset_r,
    match_probability,
    match_weight,
    original_address_concat_l,
    original_address_concat_r
    from {df_predict.physical_name}
where match_weight > 0.5
order by random()
limit 10
"""

res = linker.query_sql(sql)
display(res)

sql = f"""
SELECT *
FROM {df_predict.physical_name}
WHERE match_weight > 1
QUALIFY ROW_NUMBER() OVER (PARTITION BY unique_id_l ORDER BY match_weight DESC) = 1
ORDER BY random()
LIMIT 3
"""

recs = linker.query_sql(sql).to_dict(orient="records")


for rec in recs:
    print("-" * 80)
    print(rec["unique_id_l"], rec["original_address_concat_l"])
    print(rec["unique_id_r"], rec["original_address_concat_r"])
    display(linker.waterfall_chart([rec]))


# ------------------------------------------------------------------------------------
# Step 4: Categorise by distinguishability
# ------------------------------------------------------------------------------------
offset = 0
limit = 20
distinguishability(linker, df_predict, human_readable=True).iloc[
    offset : offset + limit
]
