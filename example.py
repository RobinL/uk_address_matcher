import duckdb
from IPython.display import display

from address_matching.cleaning import (
    add_term_frequencies_to_address_tokens,
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
from address_matching.splink_model import train_splink_model

sql = f"""
select *, address_concat as original_address_concat
from read_parquet('./example_data/companies_house_addresess_postcode_overlap.parquet')
UNION ALL
select *, address_concat as original_address_concat
from read_parquet('./example_data/fhrs_addresses_sample.parquet')
"""
df_unioned = duckdb.sql(sql)
df_unioned.df()
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


df_cleaned = run_pipeline(df_unioned, cleaning_queue, print_intermediate=False)

df_1 = df_cleaned.filter("source_dataset == 'companies_house'").df()
df_2 = df_cleaned.filter("source_dataset == 'fhrs'").df()

linker = train_splink_model(df_1, df_2)

df_predict = linker.predict(threshold_match_probability=0.5)

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

linker.comparison_viewer_dashboard("comparison_viewer_dashboard.html", overwrite=True)
linker.match_weights_chart().save("match_weights_chart.html")
