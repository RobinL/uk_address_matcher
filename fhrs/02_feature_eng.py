import os

import duckdb
import pandas as pd

df_pp = pd.read_parquet("./raw_data/price_paid_addresses.parquet")
df_pp
df_fhrs = pd.read_parquet("./raw_data/fhrs_data.parquet")
df_fhrs


sql = """
select transaction_unique_identifier as unique_id,
    'price_paid' as source_dataset,
    paon, saon, street, locality, town_city,  postcode
from df_pp
"""


df_pp_address_fields = duckdb.sql(sql)
sql = """
select
    unique_id,
    source_dataset,
    upper(concat(saon, ' ', paon, ' ', street, ' ', locality, ' ', town_city)) as address_concat,
    postcode
from df_pp_address_fields
"""
df_pp_fields_concat = duckdb.sql(sql)


sql = """
select
    fhrsid as unique_id,
    'fhrs' as source_dataset,
    addressline1,
    addressline2,
    addressline3,
    addressline4,
    postcode
from df_fhrs
"""
df_fhrs_address_fields = duckdb.sql(sql)

sql = """
select
    unique_id,
    source_dataset,
    upper(concat(addressline1, ' ', addressline2, ' ', addressline3, ' ', addressline4)) as address_concat,
    postcode
from df_fhrs_address_fields

"""
df_fhrs_fields_concat = duckdb.sql(sql)


sql = """
select
    unique_id,
    source_dataset,
    address_concat,
    postcode
from df_fhrs_fields_concat
union all
select

    unique_id,
    source_dataset,
    address_concat,
    postcode
from df_pp_fields_concat

"""
df_vertical_concat = duckdb.sql(sql)

# TODO: Could use more advance regex here to e.g. parse out numbers better
# Examples that we could better deal with:
# 'Flat 1 A' - detect that the 'number' is 1A, and this isn't two separate tokens
# '10-20 bridge street' or '10 - 20 bridge street'  - extract the number as '10-20'


sql = """
SELECT
    unique_id,
    source_dataset,
    trim(regexp_replace(
            regexp_replace(
                regexp_replace(address_concat,',', ' ', 'g'),
                '\\s*\\-\\s*', '-', 'g'), -- useful for cases like '10 - 20 bridge st'
            '[^a-zA-Z0-9 -]', '', 'g'  -- Remove anything that isn't a-zA-Z0-9 or space
        )
    ) AS address_concat_cleaned,
    address_concat,
    postcode
FROM df_vertical_concat
"""


df_all_concat_punc = duckdb.query(sql)

# Pattern to detect tokens with numbers in them inc e.g. 10-20

regex_pattern = r"\b\d*[\w\-]*\d+[\w\-]*\d*\b"

sql = f"""
SELECT
    unique_id,
    source_dataset,
    address_concat,
    regexp_replace(address_concat_cleaned, '{regex_pattern}', '', 'g')
        AS address_without_numbers,
    postcode,
    regexp_extract_all(address_concat_cleaned, '{regex_pattern}') AS numeric_tokens
FROM df_all_concat_punc
"""


df_all_numbers_in_array = duckdb.query(sql)


sql = """
select
    unique_id,
    source_dataset,
    address_concat,
    numeric_tokens[1] as numeric_token_1,
    numeric_tokens[2] as numeric_token_2,
    numeric_tokens[3] as numeric_token_3,
    trim(regexp_replace(address_without_numbers, '\\s+', ' ', 'g'))
        as address_without_numbers,
postcode
from df_all_numbers_in_array
"""

df_all_numbers_as_cols = duckdb.sql(sql)


sql = """
select
    unique_id,
    source_dataset,
    address_concat,
    numeric_token_1,
    numeric_token_2,
    numeric_token_3,
    regexp_split_to_array(trim(address_without_numbers), '\\s')
        AS address_without_numbers_tokenised,
postcode
from df_all_numbers_as_cols
"""
df_all_numbers_as_cols_others_tokenised = duckdb.sql(sql)

# If no numeric tokens, make the first token a 'number' because it's likely to be a house name
sql = """
select
    unique_id,
    source_dataset,
    address_concat,
    case
        when numeric_token_1 is null then address_without_numbers_tokenised[1]
        else numeric_token_1
        end as numeric_token_1,
    numeric_token_2,
    numeric_token_3,
    case
        when numeric_token_1 is null then address_without_numbers_tokenised[2:]
        else address_without_numbers_tokenised
    end
    as address_without_numbers_tokenised,
    postcode
from df_all_numbers_as_cols_others_tokenised
"""

df_all_numbers_as_cols_others_tokenised_2 = duckdb.sql(sql)

# Make tokens distinct
sql = """
select
    unique_id,
    source_dataset,
    address_concat,
numeric_token_1,
numeric_token_2,
numeric_token_3,
list_distinct(address_without_numbers_tokenised) as address_without_numbers_tokenised,
postcode
from df_all_numbers_as_cols_others_tokenised_2
"""
df_all_numbers_as_cols_others_tokenised_distinct = duckdb.sql(sql)

# Compute relative term frequencies amongst the tokens
sql = """
select
    token,
    count(*)  / sum(count(*)) over() as relative_frequency
from (
    select
        unnest(address_without_numbers_tokenised) as token
    from df_all_numbers_as_cols_others_tokenised_distinct
)
group by token
order by relative_frequency desc
"""
token_counts = duckdb.sql(sql)

# Add relative term frequcnies to token array
sql = """
with
addresses_exploded as (
select
    unique_id, unnest(address_without_numbers_tokenised) as token
from df_all_numbers_as_cols_others_tokenised_distinct),
address_groups as (
select addresses_exploded.*, token_counts.relative_frequency
from addresses_exploded
left join token_counts
on addresses_exploded.token = token_counts.token
)
select
    unique_id,
    list(struct_pack(token := token, relative_frequency := relative_frequency)) as token_relative_frequency_arr
from address_groups
group by unique_id

"""
adresses_with_token_relative_frequency = duckdb.sql(sql)


sql = """
select
    d.unique_id,
    d.source_dataset,
    d.address_concat,
    d.numeric_token_1,
    d.numeric_token_2,
    d.numeric_token_3,
    r.token_relative_frequency_arr,
    d.postcode
from
df_all_numbers_as_cols_others_tokenised_distinct as d
inner join adresses_with_token_relative_frequency as r
on d.unique_id = r.unique_id

"""

address_with_token_relative_frequency_arr = duckdb.sql(sql)

# Split into common and common tokens, retain token frequency only for uncommon
sql = """
select
    unique_id,
    source_dataset,
    address_concat,
    numeric_token_1,
    numeric_token_2,
    numeric_token_3,
    array_filter(token_relative_frequency_arr, x -> x.relative_frequency < 0.002) as token_relative_frequency_arr,
    array_transform(
        array_filter(token_relative_frequency_arr, x -> x.relative_frequency >= 0.002),
        x -> x.token
        ) as common_tokens,
    postcode
from address_with_token_relative_frequency_arr
"""

final = duckdb.sql(sql)


if not os.path.exists("splink_in"):
    os.makedirs("splink_in")


sql = """
COPY (
select * from final
where source_dataset = 'price_paid'
) TO 'splink_in/price_paid.parquet' (FORMAT PARQUET);
"""
duckdb.sql(sql)

sql = """
COPY (
select * from final
where source_dataset = 'fhrs'
) TO 'splink_in/fhrs.parquet' (FORMAT PARQUET);
"""
duckdb.sql(sql)
