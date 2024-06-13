import duckdb
import pandas as pd
from IPython.display import display

from address_matching.cleaning import (
    add_term_frequencies_to_address_tokens,
    clean_address_string_first_pass,
    clean_address_string_second_pass,
    final_column_order,
    first_unusual_token,
    get_token_frequeny_table,
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

# Obtained using  https://github.com/RobinL/download_openstreetmap_addresses
path = "/Users/robinlinacre/Documents/data_linking/openstreetmap_addresses/open_streetmap_addresses_deduped_1379571.parquet"
sql = f"""
SELECT
    row_number() OVER () AS unique_id,
    upper(concat_ws(' ', unit, flats, housename, housenumber, substreet, street, parentstreet, suburb, city)) as address_concat,
    postcode
from read_parquet('{path}')

"""
duckdb.sql(sql).df()
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
    get_token_frequeny_table,
]


# rel_tok_freq = run_pipeline(df_unioned, cleaning_queue, print_intermediate=False)

# # copy rel_tok_freq to parquet
# sql = """
# COPY rel_tok_freq TO 'rel_tok_freq.parquet' (FORMAT PARQUET);
# """
# duckdb.sql(sql)

# Get tf tables for numeric tokens

cleaning_queue = [
    trim_whitespace_address_and_postcode,
    upper_case_address_and_postcode,
    clean_address_string_first_pass,
    parse_out_numbers,
]


numeric_tokens_df = run_pipeline(
    df_unioned, cleaning_queue, print_intermediate=False
).df()

# For splink, table needs to be in the format
# numeric_token__2, tf_numeric_token_2_l
sql = """
with unnested as
(select unnest(numeric_tokens)  as numeric_token
from numeric_tokens_df)

select
numeric_token,
count(*)/(select count(*) from unnested) as tf_numeric_token
from  unnested
group by numeric_token
order by 2 desc

"""
numeric_token_tf_table = duckdb.sql(sql)
display(numeric_token_tf_table.df())
# copy rel_tok_freq to parquet
sql = """
COPY numeric_token_tf_table TO 'numeric_token_tf_table.parquet' (FORMAT PARQUET);
"""
duckdb.sql(sql)
