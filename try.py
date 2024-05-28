import duckdb
import pandas as pd

pd.options.display.max_colwidth = 1000


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

d1 = "companies_house_addresess_postcode_overlap"
d2 = "fhrs_addresses_sample"
sql = f"""
select *, address_concat as original_address_concat
from read_parquet('./example_data/{d1}.parquet')
UNION ALL
select *, address_concat as original_address_concat
from read_parquet('./example_data/{d2}.parquet')
"""
df_fhrs = duckdb.sql(sql)


cleaning_queue = [
    trim_whitespace_address_and_postcode,
    upper_case_address_and_postcode,
    clean_address_string_first_pass,
    parse_out_numbers,
    clean_address_string_second_pass,
    split_numeric_tokens_to_cols,
    tokenise_address_without_numbers,
    move_common_end_tokens_to_field,
    add_term_frequencies_to_address_tokens,
    first_unusual_token,
    use_first_unusual_token_if_no_numeric_token,
    final_column_order,
]


df_cleaned = run_pipeline(df_fhrs, cleaning_queue, print_intermediate=False)
pddf = df_cleaned.filter("unique_id = '724231'").df()


# """
# # duckdb.sql(sql).show(max_rows=100, max_width=10000, max_col_width=100)
# final = duckdb.sql(sql)
# # display(final.df())
# # final.filter("original_address_concat like '%FLAT%'").df()
# # df_ch = pd.read_parquet(
# #     "./example_data/companies_house_addresess_postcode_overlap.parquet"
# # )
# final.show(max_rows=10, max_width=10000, max_col_width=10000)
# df_ch_formatted = final.filter("source_dataset == 'companies_house'").df()
# # df_ch_formatted = df_ch_formatted.drop(columns=["token_rel_freq_arr"])
# df_fhrs_formatted = final.filter("source_dataset == 'fhrs'").df()
# # df_fhrs_formatted = df_fhrs_formatted.drop(columns=["token_rel_freq_arr"])

# linker = train_splink_model(df_ch_formatted, df_fhrs_formatted)

# df_predict = linker.predict(threshold_match_probability=0.001)
# df_predict.as_pandas_dataframe(limit=10)

# pd.options.display.max_rows = 100
# linker.query_sql(
#     f"""

# select
#                  unique_id_l,
#                     unique_id_r,
#                  source_dataset_l,
#                     source_dataset_r,
#                  match_probability,
#                  match_weight,
#                  original_address_concat_l,
#                  original_address_concat_r
#                   from {df_predict.physical_name}
# where match_weight > 0.5
# order by random()
# limit 100
#                  """
# )
