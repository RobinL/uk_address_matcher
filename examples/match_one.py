import duckdb
import os
from uk_address_matcher import (
    clean_data_using_precomputed_rel_tok_freq,
    get_linker,
)
from uk_address_matcher.post_linkage.analyse_results import (
    best_matches_with_distinguishability,
)
from uk_address_matcher.post_linkage.identify_distinguishing_tokens import (
    improve_predictions_using_distinguishing_tokens,
)


con = duckdb.connect(":default:")


sql = """
create or replace table df_messy as
select
    '1' as unique_id,
   '10 downing street westminster london' as address_concat,
   'SW1A 2AA' as postcode
"""


con.execute(sql)
df_messy = con.table("df_messy")

messy_count = df_messy.count("*").fetchall()[0][0]


df_messy_clean = clean_data_using_precomputed_rel_tok_freq(df_messy, con=con)

# The os.getenv can be ignored, is just so this script can be run in the test suite

full_os_path = os.getenv(
    "OS_CLEAN_PATH",
    "read_parquet('secret_data/ord_surv/os_clean.parquet')",
)

sql = f"""
create or replace view os_clean as
select *
from {full_os_path}
where postcode in (
select distinct postcode from df_messy_clean
)
"""
con.execute(sql)
df_os_clean = con.table("os_clean")


linker = get_linker(
    df_addresses_to_match=df_messy_clean,
    df_addresses_to_search_within=df_os_clean,
    con=con,
    include_full_postcode_block=True,
    include_outside_postcode_block=False,
)


df_predict = linker.inference.predict(
    threshold_match_weight=-100, experimental_optimisation=True
)
df_predict_ddb = df_predict.as_duckdbpyrelation()
df_predict_ddb.show(max_width=5000, max_rows=20)


df_predict_improved = improve_predictions_using_distinguishing_tokens(
    df_predict=df_predict_ddb,
    con=con,
    match_weight_threshold=-25,
    top_n_matches=5,
    use_bigrams=True,
)


best_matches = best_matches_with_distinguishability(
    df_predict=df_predict_improved, df_addresses_to_match=df_messy, con=con
)
best_matches.show(max_width=5000, max_rows=20)
