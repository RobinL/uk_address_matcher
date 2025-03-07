import time
import duckdb
from uk_address_matcher import (
    clean_data_using_precomputed_rel_tok_freq,
    get_linker,
)
from uk_address_matcher.post_linkage.identify_distinguishing_tokens import (
    improve_predictions_using_distinguishing_tokens,
)

overall_start_time = time.time()

# -----------------------------------------------------------------------------
# Step 1: Load data
# -----------------------------------------------------------------------------

con = duckdb.connect(":memory:")

# The os.getenv can be ignored, is just so this script can be run in the test suite
epc_path = "read_csv('secret_data/epc/raw/domestic-*/certificates.csv', filename=true)"

full_os_path = (
    "read_parquet('secret_data/ord_surv/raw/add_gb_builtaddress_sorted_zstd.parquet')"
)

labels_path = "read_csv('secret_data/epc/labels_2000.csv', all_varchar=true)"


sql = f"""
select * from {labels_path}
where confidence = 'epc_splink_agree'
and hash(messy_id) % 10 = 0
UNION ALL
select * from {labels_path}
where confidence in ('likely', 'certain')

"""
labels_filtered = con.sql(sql)
labels_filtered.count("*").fetchall()[0][0]

sql = f"""
create or replace table epc_data_raw as
select
   LMK_KEY as unique_id,
   concat_ws(' ', ADDRESS1, ADDRESS2, ADDRESS3) as address_concat,
   POSTCODE as postcode,
   UPRN as uprn,
   UPRN_SOURCE as uprn_source
from {epc_path}
where unique_id in (select messy_id from labels_filtered)

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
print(f"messy_count: {messy_count:,}, canonical_count: {canonical_count:,}")

# -----------------------------------------------------------------------------
# Step 2: Clean data
# -----------------------------------------------------------------------------


df_epc_data_clean = clean_data_using_precomputed_rel_tok_freq(df_epc_data, con=con)
df_os_clean = clean_data_using_precomputed_rel_tok_freq(df_os, con=con)

df_epc_data_clean

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
    threshold_match_weight=-50, experimental_optimisation=True
)
df_predict_ddb = df_predict.as_duckdbpyrelation()

# display(linker.visualisations.match_weights_chart())
# -----------------------------------------------------------------------------
# Step 4: Pass 2: There's an optimisation we can do post-linking to improve score
# described here https://github.com/RobinL/uk_address_matcher/issues/14
# -----------------------------------------------------------------------------


start_time = time.time()

USE_BIGRAMS = True


df_predict_improved = improve_predictions_using_distinguishing_tokens(
    df_predict=df_predict_ddb,
    con=con,
    match_weight_threshold=1,
    top_n_matches=10,
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
    l.correct_uprn as correct_uprn,
    l.confidence as label_confidence,
    case
        when m.unique_id_l = l.correct_uprn then 'true positive'
        else 'false positive'
    end as truth_status
from df_predict_ddb m
left join epc_data_raw e on m.unique_id_r = e.unique_id
left join labels_filtered l on m.unique_id_r = l.messy_id
QUALIFY ROW_NUMBER() OVER (PARTITION BY unique_id_r ORDER BY match_weight DESC) =1

"""
con.execute(sql)


sql = """
select truth_status, count(*) as count
from matches_with_epc_and_os
group by truth_status
"""
con.sql(sql).show(max_width=400, max_rows=40)
rows = con.sql(sql).fetchall()
true_positive_count = sum(row[1] for row in rows if row[0] == "true positive")
false_positive_count = sum(row[1] for row in rows if row[0] == "false positive")
perc = true_positive_count / (true_positive_count + false_positive_count)
print(
    f"True positive: {true_positive_count:,}, False positive: {false_positive_count:,}, Accuracy: {perc:.2%}"
)

sql = """
select truth_status, label_confidence, count(*) as count
from matches_with_epc_and_os
group by truth_status, label_confidence
order by label_confidence, truth_status desc
"""
con.sql(sql).show(max_width=400, max_rows=40)


sql = """
CREATE OR REPLACE TABLE matches_with_epc_and_os as

select
    m.*,
    e.uprn as epc_uprn,
    e.uprn_source as epc_source,
    l.correct_uprn as correct_uprn,
    l.confidence as label_confidence,
    case
        when m.unique_id_l = l.correct_uprn then 'true positive'
        else 'false positive'
    end as truth_status
from df_predict_improved m
left join epc_data_raw e on m.unique_id_r = e.unique_id
left join labels_filtered l on m.unique_id_r = l.messy_id
QUALIFY ROW_NUMBER() OVER (PARTITION BY unique_id_r ORDER BY match_weight DESC) =1

"""
con.execute(sql)


sql = """
select truth_status, count(*) as count
from matches_with_epc_and_os
group by truth_status
"""
con.sql(sql).show(max_width=400, max_rows=40)
rows = con.sql(sql).fetchall()
true_positive_count = sum(row[1] for row in rows if row[0] == "true positive")
false_positive_count = sum(row[1] for row in rows if row[0] == "false positive")
perc = true_positive_count / (true_positive_count + false_positive_count)
print(
    f"True positive: {true_positive_count:,}, False positive: {false_positive_count:,}, Accuracy: {perc:.2%}"
)

sql = """
select truth_status, label_confidence, count(*) as count
from matches_with_epc_and_os
group by truth_status, label_confidence
order by label_confidence, truth_status desc
"""
con.sql(sql).show(max_width=400, max_rows=40)
