import duckdb
from IPython.display import display

from address_matching.cleaning_pipelines import (
    clean_data_using_precomputed_rel_tok_freq,
)
from address_matching.splink_model import get_pretrained_linker

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
sql = """
SELECT *
FROM (
    SELECT *
    FROM read_parquet('./example_data/companies_house_addresess_postcode_overlap.parquet')
    order by postcode
    LIMIT 100
) AS companies_house
UNION ALL
SELECT *
FROM (
    SELECT *
    FROM read_parquet('./example_data/fhrs_addresses_sample.parquet')
    order by postcode
    LIMIT 100
) AS fhrs

"""

address_table = con.sql(sql)


# -----------------------------------------------------------------------------
# Step 2: Clean the data/feature engineering to prepare for matching model
# -----------------------------------------------------------------------------


# You have the option of passing in precomputed term frequcnies.
# This is particularly important if you have a small dataset where toekn frequencies
# will not be representative of the population
# This tf table is created using
# .token_and_term_frequencies.get_address_token_frequencies_from_address_table
path = "./example_data/rel_tok_freq.parquet"
rel_tok_freq = con.sql(f"SELECT token, rel_freq FROM read_parquet('{path}')")

# This is how you'd run data cleaning:
cleaned_address_2 = clean_data_using_precomputed_rel_tok_freq(
    address_table, rel_tok_freq_table=rel_tok_freq, con=con
)

df_1 = cleaned_address_2.filter("source_dataset == 'companies_house'")
df_2 = cleaned_address_2.filter("source_dataset == 'fhrs'")


# Use pre-computed numeric token frequencies
# This tf table created using
# .token_and_term_frequenciesget_numeric_term_frequencies_from_address_table
path = "./example_data/numeric_token_tf_table.parquet"
sql = f"""
SELECT *
FROM read_parquet('{path}')
"""
numeric_token_freq = con.sql(sql)


# All dfs going in here are of type DuckDBPyRelation
# See note at end re: using precomputed term frequencies
linker = get_pretrained_linker(
    [df_1, df_2], precomputed_numeric_tf_table=numeric_token_freq, con=con
)
df_predict = linker.predict(threshold_match_probability=0.9)

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
select * from {df_predict.physical_name}
where match_weight > 0.5
order by random()
limit 3
"""
recs = linker.query_sql(sql).to_dict(orient="records")


for rec in recs:
    print("-" * 80)
    print(rec["unique_id_l"], rec["original_address_concat_l"])
    print(rec["unique_id_r"], rec["original_address_concat_r"])
    display(linker.waterfall_chart([rec]))
