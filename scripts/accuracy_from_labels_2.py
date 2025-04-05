from IPython.display import display
import time
import duckdb
from uk_address_matcher import (
    clean_data_using_precomputed_rel_tok_freq,
    get_linker,
)
from uk_address_matcher.post_linkage.identify_distinguishing_tokens import (
    improve_predictions_using_distinguishing_tokens,
)
from uk_address_matcher.linking_model.training import get_settings_for_training
import random
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


# TODO: Print headline stats of results here


# # -----------------------------------------------------------------------------
# # Step 6: Inspect single rows
# # This code is specific to the EPC data where we have a UPRN from the EPC data
# # that we can compare the the one we found
# # -----------------------------------------------------------------------------

# For each unique_id_r, print a report of performance

sql = """
select distinct unique_id_r
from df_predict_with_distinguishability
where correct_uprn_l != correct_uprn_r
"""
unique_id_r_list = con.sql(sql).fetchall()
unique_id_r_list = [x[0] for x in unique_id_r_list]
random.shuffle(unique_id_r_list)
this_id = unique_id_r_list[0]


sql = f"""
select d.*, o.address_concat as label_address_concat, o.postcode as label_postcode
from df_predict_with_distinguishability as d
left join df_os as o
on d.correct_uprn_r = o.unique_id
where d.unique_id_r = '{this_id}'
"""
this_match = con.sql(sql)

row_dict_best_match = dict(zip(this_match.columns, this_match.fetchone()))


sql = f"""
select *
from df_predict_improved
where correct_uprn_l = correct_uprn_r
and unique_id_r = '{this_id}'
"""
correct_match = con.sql(sql)
row_dict_correct_match = dict(zip(correct_match.columns, correct_match.fetchone()))
row_dict_correct_match


report_template = """
===========================================================================
unique_id_r:                  {this_id}
{messy_label:<30}{messy_address} {messy_postcode}

{best_match_label:<30}{best_match_address} {best_match_postcode} (UPRN: {best_match_uprn})
{true_match_label:<30}{true_match_address} {true_match_postcode} (UPRN: {true_match_uprn})
Distinguishability:           {distinguishability_value}
"""

report = report_template.format(
    this_id=this_id,
    messy_label="Messy address:",
    best_match_label=f"Best match (score: {row_dict_best_match['match_weight']:,.2f}):",
    true_match_label=f"True match (score: {row_dict_correct_match['match_weight']:,.2f}):",
    messy_address=row_dict_best_match["address_concat_r"],
    best_match_address=row_dict_best_match["original_address_concat_l"],
    true_match_address=row_dict_best_match["label_address_concat"],
    distinguishability_value=f"{row_dict_best_match['distinguishability']:,.2f}",
    messy_postcode=row_dict_best_match["postcode_r"],
    best_match_postcode=row_dict_best_match["postcode_l"],
    true_match_postcode=row_dict_best_match["label_postcode"],
    best_match_uprn=row_dict_best_match["correct_uprn_l"],
    true_match_uprn=row_dict_best_match["correct_uprn_r"],
)
print(report)

sql = f"""
SELECT
    original_address_concat_r,
    case
        when correct_uprn_l = correct_uprn_r then concat('✅ ', original_address_concat_l)
        else original_address_concat_l
    end as address_concat_l,
    match_weight,
    mw_adjustment,


    * EXCLUDE (
        original_address_concat_r,
        original_address_concat_l,
        match_weight,
        mw_adjustment,
        unique_id_r,
        correct_uprn_l,
        correct_uprn_r,

    )
FROM df_predict_improved
WHERE unique_id_r = '{this_id}'
order by match_weight desc
"""
con.sql(sql).show(max_width=10000)

# Printed cleaned data for best match and true match

to_select = """
    original_address_concat,
    flat_positional,
    flat_letter,
    numeric_token_1,
    numeric_token_2,
    numeric_token_3,
    unusual_tokens_arr,
    very_unusual_tokens_arr,
    extremely_unusual_tokens_arr,
    * EXCLUDE (
        original_address_concat,
        flat_positional,
        flat_letter,
        numeric_token_1,
        numeric_token_2,
        numeric_token_3,
        unusual_tokens_arr,
        very_unusual_tokens_arr,
        extremely_unusual_tokens_arr
    )
"""

sql = f"""
SELECT 'Messy record' AS rec, {to_select}
FROM df_messy_data_clean
WHERE unique_id = '{this_id}'

UNION ALL

SELECT 'Best match' AS rec, {to_select}
FROM df_os_clean
WHERE unique_id = '{row_dict_best_match["unique_id_l"]}'

UNION ALL

SELECT 'True match' AS rec, {to_select}
FROM df_os_clean
WHERE unique_id = '{row_dict_correct_match["unique_id_l"]}'
"""

con.sql(sql).show(max_width=10000)


waterfall_header = """
Waterfall chart for messy address vs best match:
{messy_address} {messy_postcode}
{best_match_address} {best_match_postcode}
"""

waterfall_header = waterfall_header.format(
    messy_address=row_dict_best_match["address_concat_r"],
    messy_postcode=row_dict_best_match["postcode_r"],
    best_match_address=row_dict_best_match["original_address_concat_l"],
    best_match_postcode=row_dict_best_match["postcode_l"],
)

print(waterfall_header)

best_uprn = row_dict_best_match["unique_id_l"]
sql = f"""
select *
from df_predict_ddb
where unique_id_r = '{this_id}' and unique_id_l = '{best_uprn}'
order by match_weight desc
limit 1
"""

res = con.sql(sql)


display(
    linker.visualisations.waterfall_chart(
        res.df().to_dict(orient="records"), filter_nulls=False
    )
)


waterfall_header = """
Waterfall chart for messy address vs true match:
{messy_address} {messy_postcode}
{true_match_address} {true_match_postcode}
"""

waterfall_header = waterfall_header.format(
    messy_address=row_dict_best_match["address_concat_r"],
    messy_postcode=row_dict_best_match["postcode_r"],
    true_match_address=row_dict_correct_match["original_address_concat_l"],
    true_match_postcode=row_dict_correct_match["postcode_l"],
)
print(waterfall_header)

correct_uprn = row_dict_correct_match["correct_uprn_l"]
sql = f"""
select *
from df_predict_ddb
where unique_id_r = '{this_id}' and unique_id_l = '{correct_uprn}'
order by match_weight desc
limit 1
"""

res = con.sql(sql)


display(
    linker.visualisations.waterfall_chart(
        res.df().to_dict(orient="records"), filter_nulls=False
    )
)
