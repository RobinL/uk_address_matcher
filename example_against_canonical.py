import duckdb
import pandas as pd

from uk_address_matcher.analyse_results import (
    distinguishability_by_id,
    distinguishability_summary,
)
from uk_address_matcher.cleaning_pipelines import (
    clean_data_using_precomputed_rel_tok_freq,
)
from uk_address_matcher.splink_model_vs_canonical import (
    _performance_predict_against_canonical,
)

con = duckdb.connect("./canonical_all.ddb")


sql = """
create or replace table full_blocking_canonical as
select
unique_id,
numeric_token_1,
numeric_token_2,
numeric_token_3,
numeric_1_alt,
postcode,
list_extract(unusual_tokens_arr, 1) as le_unusual_tokens_arr_1,
list_extract(unusual_tokens_arr, 2) as le_unusual_tokens_arr_2,
list_extract(very_unusual_tokens_arr, 1) as le_very_unusual_tokens_arr_1,
list_extract(very_unusual_tokens_arr, 2) as le_very_unusual_tokens_arr_2,
list_extract(extremely_unusual_tokens_arr, 1) as le_extremely_unusual_tokens_arr_1,
split_part(postcode, ' ', 1) as postcode_start,
split_part(postcode, ' ', 2) as postcode_end,
very_unusual_tokens_arr,
extremely_unusual_tokens_arr
from full_canonical
"""
con.sql(sql)


sql = """
create or replace table os_numeric_tf as
select * from
read_parquet('/path_to/os_numeric_freq.parquet')
"""
con.sql(sql)
numeric_tf = con.table("os_numeric_tf")


p_ch = "./example_data/companies_house_addresess_postcode_overlap.parquet"
df_ch = con.read_parquet(p_ch).order("postcode").limit(20000)

new_recs_clean = clean_data_using_precomputed_rel_tok_freq(df_ch, con=con)

recs = [
    {
        "unique_id": "1",
        "source_dataset": "other",
        "address_concat": "102 Petty France",
        "postcode": "SW1H 9AJ",
    }
]
new_recs_clean = clean_data_using_precomputed_rel_tok_freq(pd.DataFrame(recs), con)


pd.options.display.max_columns = 1000
pd.options.display.max_rows = 100

new_recs_clean.df()


predictions = _performance_predict_against_canonical(
    df_addresses_to_match=new_recs_clean,
    tf_table=numeric_tf,
    con=con,
    match_weight_threshold=None,
    output_all_cols=True,
    include_full_postcode_block=False,
)
predictions.count("*")


distinguishability_summary(
    df_predict=predictions, df_addresses_to_match=new_recs_clean, con=con
)

sql = f"""
select
    unique_id_l,
    unique_id_r,
    source_dataset_l,
    source_dataset_r,
    match_probability,
    match_weight,
    original_address_concat_l,
    original_address_concat_r,
    postcode_l,
    postcode_r
    from predictions

order by match_weight desc
limit 10
"""

con.sql(sql).df()

pd.options.display.max_colwidth = 1000
res = distinguishability_by_id(predictions, new_recs_clean, con)
res.df()
