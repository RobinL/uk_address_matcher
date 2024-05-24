# %load_ext autoreload
# %autoreload 2


import duckdb
import pandas as pd
from duckdb import DuckDBPyConnection, DuckDBPyRelation

from address_matching.cleaning import (
    add_term_frequencies_to_address_tokens,
    clean_address_string_first_pass,
    clean_address_string_second_pass,
    first_unusual_token,
    parse_out_numbers,
    split_numeric_tokens_to_cols,
    tokenise_address_without_numbers,
    trim_whitespace_address_and_postcode,
    upper_case_address_and_postcode,
    use_first_unusual_token_if_no_numeric_token,
)
from address_matching.splink_model import train_splink_model

pd.options.display.max_colwidth = 1000


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
duckdb.register("df_fhrs", df_fhrs)


cleaning_queue = [
    trim_whitespace_address_and_postcode,
    upper_case_address_and_postcode,
    clean_address_string_first_pass,
    parse_out_numbers,
    clean_address_string_second_pass,
    split_numeric_tokens_to_cols,
    tokenise_address_without_numbers,
    add_term_frequencies_to_address_tokens,
    first_unusual_token,
    use_first_unusual_token_if_no_numeric_token,
]
print_intermediate = False

table_name = "df_fhrs"

# Have to register name to avoid recursion error
for i, cleaning_function in enumerate(cleaning_queue):
    df = cleaning_function(table_name)
    table_name = f"df_{i}"
    duckdb.register(table_name, df)
    if print_intermediate:
        # display(df.df().sample(10))
        # print("------------")
        df.show(max_rows=10, max_width=10000, max_col_width=10000)
        # df.filter("original_address_concat like '%FLAT%'").show(
        #     max_rows=5, max_width=10000, max_col_width=10000
        # )
# df.show(max_rows=100, max_width=10000, max_col_width=100)

sql = f"""
select
    unique_id,
    source_dataset,
    original_address_concat,
    numeric_token_1,
    numeric_token_2,
    numeric_token_3,
    list_transform(
        token_rel_freq_arr, x-> struct_pack(t:= x[1], v:= x[2])
    ) as token_rel_freq_arr,
    postcode
from {table_name}

"""
# duckdb.sql(sql).show(max_rows=100, max_width=10000, max_col_width=100)
final = duckdb.sql(sql)
# display(final.df())
# final.filter("original_address_concat like '%FLAT%'").df()
# df_ch = pd.read_parquet(
#     "./example_data/companies_house_addresess_postcode_overlap.parquet"
# )
final.show(max_rows=10, max_width=10000, max_col_width=10000)
df_ch_formatted = final.filter("source_dataset == 'companies_house'").df()
# df_ch_formatted = df_ch_formatted.drop(columns=["token_rel_freq_arr"])
df_fhrs_formatted = final.filter("source_dataset == 'fhrs'").df()
# df_fhrs_formatted = df_fhrs_formatted.drop(columns=["token_rel_freq_arr"])

linker = train_splink_model(df_ch_formatted, df_fhrs_formatted)

df_predict = linker.predict(threshold_match_probability=0.001)
