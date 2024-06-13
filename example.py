import duckdb
from IPython.display import display

from address_matching.cleaning_pipelines import (
    clean_data_using_precomputed_rel_tok_freq,
)
from address_matching.splink_model import get_pretrained_linker, train_splink_model

# Address tables for cleaning
sql = """
SELECT *, address_concat AS original_address_concat
FROM (
    SELECT *, address_concat AS original_address_concat
    FROM read_parquet('./example_data/companies_house_addresess_postcode_overlap.parquet')
    order by postcode
    LIMIT 100
) AS companies_house
UNION ALL
SELECT *, address_concat AS original_address_concat
FROM (
    SELECT *, address_concat AS original_address_concat
    FROM read_parquet('./example_data/fhrs_addresses_sample.parquet')
    order by postcode
    LIMIT 100
) AS fhrs

"""

address_table = duckdb.sql(sql)

# Load in pre-computed token frequencies.  This allows the model to operatre
# even if your dataset is small/unrepresentative
# Created using
# .token_and_term_frequencies.get_address_token_frequencies_from_address_table
path = "./rel_tok_freq.parquet"
rel_tok_freq = duckdb.sql(f"SELECT token, rel_freq FROM read_parquet('{path}')")

ddb_df = clean_data_using_precomputed_rel_tok_freq(
    address_table, rel_tok_freq_table=rel_tok_freq
)

df_1 = ddb_df.filter("source_dataset == 'companies_house'").df()
df_2 = ddb_df.filter("source_dataset == 'fhrs'").df()

# Created using
# .token_and_term_frequenciesget_numeric_term_frequencies_from_address_table
path = "./numeric_token_tf_table.parquet"
sql = f"""
SELECT *
FROM read_parquet('{path}')
"""
numeric_token_freq = duckdb.sql(sql)

# If you want to train a model you can do this instead
# linker = train_splink_model(df_1, df_2, retain_original_address_concat=True)

linker = get_pretrained_linker(
    [df_1, df_2], precomputed_numeric_tf_table=numeric_token_freq
)
linker.match_weights_chart()
df_predict = linker.predict(threshold_match_probability=0.9)


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
res

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

linker.comparison_viewer_dashboard(
    df_predict, overwrite=True, out_path="comparison_viewer_dashboard.html"
)
linker.match_weights_chart().save("match_weights_chart.html")
