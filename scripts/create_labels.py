import time
import os
import duckdb
from uk_address_matcher import (
    clean_data_using_precomputed_rel_tok_freq,
    get_linker,
)
from uk_address_matcher.post_linkage.analyse_results import (
    best_matches_summary,
    best_matches_with_distinguishability,
)
from uk_address_matcher.post_linkage.identify_distinguishing_tokens import (
    improve_predictions_using_distinguishing_tokens,
)

overall_start_time = time.time()

# -----------------------------------------------------------------------------
# Step 1: Load data
# -----------------------------------------------------------------------------

con = duckdb.connect(":default:")

# The os.getenv can be ignored, is just so this script can be run in the test suite
epc_path = os.getenv(
    "EPC_PATH",
    "read_csv('secret_data/epc/raw/domestic-*/certificates.csv', filename=true)",
)
full_os_path = os.getenv(
    "FULL_OS_PATH",
    "read_parquet('secret_data/ord_surv/raw/add_gb_builtaddress_sorted_zstd.parquet')",
)


sql = f"""
create or replace table epc_data_raw as
select
   LMK_KEY as unique_id,
   concat_ws(' ', ADDRESS1, ADDRESS2, ADDRESS3) as address_concat,
   POSTCODE as postcode,
   UPRN as uprn,
   UPRN_SOURCE as uprn_source
from {epc_path}
-- where lower(filename) like '%hammersmith%'
-- limit 1000
"""
con.execute(sql)

sql = f"""
create or replace table os as
select
   uprn as unique_id,
   regexp_replace(fulladdress, ',[^,]*$', '') AS address_concat,
   postcode
from {full_os_path}
where postcode in
(select distinct postcode from epc_data_raw)
and
description != 'Non Addressable Object'

"""
con.execute(sql)
df_os = con.table("os")


df_epc_data = con.sql("select * exclude (uprn,uprn_source) from epc_data_raw")


messy_count = df_epc_data.count("*").fetchall()[0][0]
canonical_count = df_os.count("*").fetchall()[0][0]


# -----------------------------------------------------------------------------
# Step 2: Clean data
# -----------------------------------------------------------------------------


df_epc_data_clean = clean_data_using_precomputed_rel_tok_freq(df_epc_data, con=con)
df_os_clean = clean_data_using_precomputed_rel_tok_freq(df_os, con=con)

end_time = time.time()
print(f"Time to load/clean: {end_time - overall_start_time} seconds")

# -----------------------------------------------------------------------------
# Step 3: Link data - pass 1
# -----------------------------------------------------------------------------


linker = get_linker(
    df_addresses_to_match=df_epc_data_clean,
    df_addresses_to_search_within=df_os_clean,
    con=con,
    include_full_postcode_block=True,
    include_outside_postcode_block=True,
    retain_intermediate_calculation_columns=True,
)


df_predict = linker.inference.predict(
    threshold_match_weight=-100, experimental_optimisation=True
)
df_predict_ddb = df_predict.as_duckdbpyrelation()

# -----------------------------------------------------------------------------
# Step 4: Pass 2: There's an optimisation we can do post-linking to improve score
# described here https://github.com/RobinL/uk_address_matcher/issues/14
# -----------------------------------------------------------------------------


start_time = time.time()

USE_BIGRAMS = True


df_predict_improved = improve_predictions_using_distinguishing_tokens(
    df_predict=df_predict_ddb,
    con=con,
    match_weight_threshold=-15,
    top_n_matches=5,
    use_bigrams=USE_BIGRAMS,
)


end_time = time.time()
print(f"Improve time taken: {end_time - start_time} seconds")
print(
    f"Full time taken: {end_time - overall_start_time} seconds to match "
    f"{messy_count:,.0f} messy addresses to {canonical_count:,.0f} canonical addresses "
    f"at a rate of {messy_count / (end_time - overall_start_time):,.0f} "
    "addresses per second"
)

# -----------------------------------------------------------------------------
# Step 5: Inspect results
# -----------------------------------------------------------------------------

dsum_1 = best_matches_summary(
    df_predict=df_predict_ddb,
    df_addresses_to_match=df_epc_data,
    con=con,
)
dsum_1.show(max_width=500)


# # -----------------------------------------------------------------------------
# # Step 6: Inspect single rows
# # This code is specific to the EPC data where we have a UPRN from the EPC data
# # that we can compare the the one we found
# # -----------------------------------------------------------------------------

sql = """
CREATE OR REPLACE TABLE matches_with_epc_and_os as

select
    m.*,
    e.uprn as epc_uprn,
    e.uprn_source as epc_source,
    concat(o.original_address_concat, ' ', o.postcode) as os_address,
    case
        when trim(o.original_address_concat) = '' or o.original_address_concat is null
        then 'no_os_address'
        when e.uprn = unique_id_l then 'agree'
        else 'disagree'
    end as ours_theirs_agreement
from df_predict_improved m
left join epc_data_raw e on m.unique_id_r = e.unique_id
left join df_os_clean o on e.uprn = o.unique_id
QUALIFY ROW_NUMBER() OVER (PARTITION BY unique_id_r ORDER BY match_weight DESC) =1

"""
con.execute(sql)

con.table("matches_with_epc_and_os").show(max_width=1000)


con = duckdb.connect()
sql = """
select *
from read_json('address_matching_results_2000.jsonl')
"""

con.sql(sql).show(max_width=1000)


sql = """
select unique_id_l as correct_uprn, unique_id_r as messy_id, 'epc_splink_agree' as confidence
from matches_with_epc_and_os
where ours_theirs_agreement = 'agree'

UNION ALL

select correct_uprn, messy_id,  confidence
from read_json('address_matching_results_2000.jsonl')

"""

labels = con.sql(sql)
labels

sql = """
select confidence, count(*)
from labels
group by 1
"""

con.sql(sql).show(max_width=1000)


labels.to_csv("secret_data/epc/labels_2000.csv")
