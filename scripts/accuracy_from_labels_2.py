import time
from IPython.display import display
import duckdb
from uk_address_matcher import (
    clean_data_using_precomputed_rel_tok_freq,
    get_linker,
)
from uk_address_matcher.post_linkage.identify_distinguishing_tokens import (
    improve_predictions_using_distinguishing_tokens,
)
from uk_address_matcher.linking_model.training import get_settings_for_training
from uk_address_matcher.post_linkage.analyse_results import (
    best_matches_with_distinguishability,
)


con = duckdb.connect()

full_os_path = (
    "read_parquet('secret_data/ord_surv/raw/add_gb_builtaddress_sorted_zstd.parquet')"
)

overall_start_time = time.time()
path = "secret_data/labels/*.jsonl"


sql = f"""
CREATE TABLE labels_raw AS
select * from
read_json('{path}', filename=labels_filename, columns={{
    'messy_id': 'VARCHAR',
    'correct_uprn': 'VARCHAR',
    'confidence': 'VARCHAR'
}})
where  not contains(correct_uprn, '[')
and nullif(correct_uprn, '') is not null
and try_cast(correct_uprn as BIGINT) in (select uprn from {full_os_path})
"""
con.execute(sql)

sql = """
CREATE TABLE labels_all AS

-- Labels from LLM labelling of fhrs data
select *
from labels_raw
WHERE CONTAINS(labels_filename, 'fhrs') and CONTAINS(labels_filename, 'llm')
and confidence = 'certain'

UNION ALL
-- Labels from LLM labelling of EPC data where splink and EPC did not match
select *
from labels_raw
WHERE CONTAINS(labels_filename, 'epc') and CONTAINS(labels_filename, 'llm')
and confidence = 'certain'

UNION ALL
-- Labels from auto agreement of EPC and splink matcher
SELECT * FROM (
    SELECT
        *
    FROM labels_raw
    WHERE CONTAINS(labels_filename, 'epc') AND CONTAINS(labels_filename, 'auto')
    and
    messy_id not in
    (
    select messy_id from labels_raw
     WHERE CONTAINS(labels_filename, 'epc') and CONTAINS(labels_filename, 'llm')
    )
    ORDER BY substr(messy_id, 4, 4)
    LIMIT 3000
)
"""
con.execute(sql)
labels = con.table("labels_all")
labels.aggregate(
    "coalesce(labels_filename, 'total') as labels_count, count(*)",
    "cube(labels_filename)",
).show()

sql = f"""
select labels.*, os.fulladdress from labels
inner join {full_os_path} os
on try_cast(correct_uprn as BIGINT) = os.uprn
"""
labels_with_os = con.sql(sql)
labels_with_os.count("*").show()
labels_with_os
# labels like
# ┌────────────────────────────────┬──────────────┬──────────────────────┬───────────────────────────────────────────────┐
# │            messy_id            │ correct_uprn │      confidence      │                labels_filename                │
# │            varchar             │   varchar    │       varchar        │                    varchar                    │
# ├────────────────────────────────┼──────────────┼──────────────────────┼───────────────────────────────────────────────┤
# │ 1000661                        │ 100052007636 │ certain              │ secret_data/labels/llm_labels_3000_fhrs.jsonl │
# │ 1

epc_path = (
    "read_csv('secret_data/epc/raw/domestic-*/certificates.csv', filename=epc_filename)"
)


fhrs_path = "read_parquet('secret_data/fhrs/fhrs_data.parquet')"

sql = f"""
select DISTINCT ON (fhrsid) *
from {fhrs_path}
"""
fhrs_deduped = con.sql(sql)

# FHRS data has duplicate rows for some reason


sql = f"""
CREATE OR REPLACE TABLE messy_data AS
-- Labels from LLM labelling of fhrs data
SELECT
    f.fhrsid AS unique_id,
    CONCAT_WS(' ', f.BusinessName, f.AddressLine1, f.AddressLine2, f.AddressLine3, f.AddressLine4) AS address_concat,
    f.PostCode AS postcode,
    l.labels_filename,
    l.correct_uprn AS correct_uprn
FROM fhrs_deduped f
INNER JOIN labels l ON f.fhrsid = l.messy_id
WHERE CONTAINS(l.labels_filename, 'fhrs') AND CONTAINS(l.labels_filename, 'llm')

UNION ALL

-- Labels from LLM labelling of EPC data where splink and EPC did not match
SELECT
    e.LMK_KEY AS unique_id,
    CONCAT_WS(' ', e.ADDRESS1, e.ADDRESS2, e.ADDRESS3) AS address_concat,
    e.POSTCODE AS postcode,
    l.labels_filename,
    l.correct_uprn AS correct_uprn
FROM {epc_path} e
INNER JOIN labels l ON e.LMK_KEY = l.messy_id
WHERE CONTAINS(l.labels_filename, 'epc') AND CONTAINS(l.labels_filename, 'llm')

UNION ALL

-- Labels from auto agreement of EPC and splink matcher
SELECT
    e.LMK_KEY AS unique_id,
    CONCAT_WS(' ', e.ADDRESS1, e.ADDRESS2, e.ADDRESS3) AS address_concat,
    e.POSTCODE AS postcode,
    l.labels_filename,
    l.correct_uprn AS correct_uprn
FROM {epc_path} e
INNER JOIN labels l ON e.LMK_KEY = l.messy_id
WHERE CONTAINS(l.labels_filename, 'epc') AND CONTAINS(l.labels_filename, 'auto')

"""

con.execute(sql)
messy_data = con.table("messy_data")
messy_data

messy_data.aggregate(
    "coalesce(labels_filename, 'total') as messy_data_count, count(*)",
    "cube(labels_filename)",
).show()


# Messy data like
# ┌──────────────────────┬────────────────────────────────────┬──────────┬───────────────────────────────────────────────┐
# │      unique_id       │           address_concat           │ postcode │                labels_filename                │
# │       varchar        │              varchar               │ varchar  │                    varchar                    │
# ├──────────────────────┼────────────────────────────────────┼──────────┼───────────────────────────────────────────────┤
# │ 52714                │ White Heather Hotel White Heathe…  │ LL30 2NR │ secret_data/labels/llm_labels_3000_fhrs.jsonl │
# │ 1701094              │ Champs Cafe Champs Cafe 64 Drome…  │ CH5 2LR  │ secret_data/labels/llm_labels_3000_fhrs.jsonl │


sql = f"""
create or replace table os as
select
uprn as unique_id,
regexp_replace(fulladdress, ',[^,]*$', '') AS address_concat,

postcode,
uprn as correct_uprn,
from {full_os_path}
where
(postcode in (select distinct postcode from messy_data))
or
(uprn in (select try_cast(correct_uprn as BIGINT) from labels))


"""
con.execute(sql)
df_os = con.table("os")

# df os like:
# ┌─────────────┬────────────────────────────────────────────────────────────────────────────────────┬──────────┐
# │  unique_id  │                                   address_concat                                   │ postcode │
# │    int64    │                                      varchar                                       │ varchar  │
# ├─────────────┼────────────────────────────────────────────────────────────────────────────────────┼──────────┤
# │    25035526 │ 10,  SOME ADDRESS GOES HERE.                                                       │ MK14 6EF │


messy_count = messy_data.count("*").fetchall()[0][0]
canonical_count = df_os.count("*").fetchall()[0][0]
print(f"messy_count: {messy_count:,}, canonical_count: {canonical_count:,}")


# -----------------------------------------------------------------------------
# Step 2: Clean data
# -----------------------------------------------------------------------------

df_messy_data_clean = clean_data_using_precomputed_rel_tok_freq(messy_data, con=con)
sql = """
create table messy_data_clean as
select * exclude (labels_filename) from df_messy_data_clean
"""
con.execute(sql)
df_messy_data_clean = con.table("messy_data_clean")


df_os_clean = clean_data_using_precomputed_rel_tok_freq(df_os, con=con)
sql = """
create table os_clean as
select * from df_os_clean
"""
con.execute(sql)


# -----------------------------------------------------------------------------
# Step 3: Link data - pass 1
# -----------------------------------------------------------------------------
settings = get_settings_for_training()

linker = get_linker(
    df_addresses_to_match=df_messy_data_clean,
    df_addresses_to_search_within=df_os_clean,
    con=con,
    include_full_postcode_block=True,
    include_outside_postcode_block=True,
    retain_intermediate_calculation_columns=True,
    additional_columns_to_retain=["correct_uprn"],
    settings=settings,
)


df_predict = linker.inference.predict(
    threshold_match_weight=-50, experimental_optimisation=True
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
    match_weight_threshold=-10,
    top_n_matches=5,
    use_bigrams=USE_BIGRAMS,
    additional_columns_to_retain=["correct_uprn"],
)


end_time = time.time()
print(f"Improve time taken: {end_time - start_time} seconds")
print(
    f"Full time taken: {end_time - overall_start_time} seconds to match "
    f"{messy_count:,.0f} messy addresses to {canonical_count:,.0f} canonical addresses "
    f"at a rate of {messy_count / (end_time - overall_start_time):,.0f} "
    "addresses per second"
)

df_predict_with_distinguishability = best_matches_with_distinguishability(
    df_predict=df_predict_improved,
    df_addresses_to_match=messy_data,
    con=con,
    additional_columns_to_retain=["correct_uprn"],
)
df_predict_with_distinguishability.show(max_width=10000)
# # -----------------------------------------------------------------------------
# # Step 6: Inspect single rows
# # This code is specific to the EPC data where we have a UPRN from the EPC data
# # that we can compare the the one we found
# # -----------------------------------------------------------------------------


sql = """
CREATE OR REPLACE TABLE matches_with_label as
WITH ranked_matches AS (
    SELECT
        m.*,
        l.correct_uprn AS correct_uprn,
        l.confidence AS label_confidence,
        l.fulladdress AS labels_fulladdress,
        l.labels_filename AS labels_filename,
        CASE
            WHEN m.unique_id_l = l.correct_uprn THEN 'true positive'
            ELSE 'false positive'
        END AS truth_status,
        ROW_NUMBER() OVER (
            PARTITION BY unique_id_r
            ORDER BY match_weight DESC, unique_id_l
        ) AS rn
    FROM df_predict_ddb m
    LEFT JOIN labels_with_os l ON m.unique_id_r = l.messy_id
)
SELECT *
FROM ranked_matches
WHERE (rn = 1 OR unique_id_l = correct_uprn)

"""
con.execute(sql)
matches_with_label = con.table("matches_with_label")
matches_with_label.show(max_width=10000)


sql = """
select distinct unique_id_r
from matches_with_label
where not (rn=1 and truth_status = 'true positive')
order by random()
"""
id_rs = con.sql(sql).fetchall()
id_rs = [x[0] for x in id_rs]


for this_id in id_rs[:3]:
    this_match_relation = matches_with_label.filter(f"unique_id_r = '{this_id}'")
    columns = this_match_relation.columns
    rows = this_match_relation.fetchall()

    if not rows:
        continue

    results = [dict(zip(columns, row)) for row in rows]

    address_r = results[0].get("original_address_concat_r", "")
    postcode_r = results[0].get("postcode_r", "")
    truth_address_label = results[0].get("labels_fulladdress", "N/A")

    input_addr_full = f"{address_r} {postcode_r}".strip()

    tp_address_l = "N/A"
    tp_postcode_l = ""
    tp_weight = None

    highest_fp_weight = -float("inf")
    fp_address_l_best = "N/A"
    fp_postcode_l_best = ""
    fp_weight_best = None

    for r in results:
        status = r.get("truth_status")
        address_l = r.get("original_address_concat_l", "")
        postcode_l = r.get("postcode_l", "")
        weight = r.get("match_weight")

        if status == "true positive":
            # In case multiple TPs somehow exist, overwrite (or take first)
            tp_address_l = address_l
            tp_postcode_l = postcode_l
            tp_weight = weight
        elif status == "false positive":
            if weight is not None and weight > highest_fp_weight:
                highest_fp_weight = weight
                fp_address_l_best = address_l
                fp_postcode_l_best = postcode_l
                fp_weight_best = weight

    # Use the actual TP label address, but the TP match weight
    # tp_addr_full = f"{tp_address_l} {tp_postcode_l}".strip() # This would be the matched address for TP
    truth_addr_display = truth_address_label  # Use the ground truth string

    fp_addr_full = (
        f"{fp_address_l_best} {fp_postcode_l_best}".strip()
        if fp_address_l_best != "N/A"
        else "None Found"
    )

    # Format weights, aligning them
    weight_padding = 8
    tp_weight_str = (
        f"{tp_weight:.4f}".ljust(weight_padding)
        if tp_weight is not None
        else "N/A".ljust(weight_padding)
    )
    fp_weight_str = (
        f"{fp_weight_best:.4f}".ljust(weight_padding)
        if fp_weight_best is not None
        else "N/A".ljust(weight_padding)
    )

    label_padding = 28
    to_print = "=" * 80 + "\n"
    to_print += f"Comparison Report for unique_id_r: {this_id}\n"
    to_print += "-" * 80 + "\n"
    to_print += f"{'Input Address (Right):'.ljust(label_padding)}{''.ljust(weight_padding)}  {input_addr_full}\n"
    to_print += f"{'False positive address:'.ljust(label_padding)}{fp_weight_str}  {fp_addr_full}\n"
    to_print += f"{'Ground Truth Address:'.ljust(label_padding)}{tp_weight_str}  {truth_addr_display}\n"
    to_print += "=" * 80 + "\n"

    print(to_print)
