# Feature engineering the the downloaded data

# Going to largely follow the approach discussed here:
# https://github.com/moj-analytical-services/splink/discussions/2022

# But will separately parse out tokens that include numbers so we can build
# more sophisticated logic for those fields

import os

import duckdb
import pandas as pd

pd.options.display.max_columns = None
pd.options.display.max_colwidth = None


df_pp = pd.read_parquet("./raw_address_data/price_paid_addresses.parquet")
df_pp
df_epc = pd.read_parquet("./raw_address_data/adur_epc.parquet")
df_epc

where_statement = """
where postcode = 'BN15 8HQ'
and paon = 78
"""
where_statement = ""

# district, county fields exist but probably not useful
sql = f"""
select transaction_unique_identifier as unique_id,
    'price_paid' as source_dataset,
    paon, saon, street, locality, town_city,  postcode
from df_pp
{where_statement}
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
df_pp_fields_concat.df()

# LOCAL_AUTHORITY_LABEL exists, but isn't really uesful

where_statement = """
where postcode = 'BN15 8HQ'
and address1 like '%78%'
"""
where_statement = ""


sql = f"""
select
LMK_KEY as unique_id,
'epc' as source_dataset,
address1, address2, address3, POSTTOWN,  postcode
from df_epc
{where_statement}
"""
df_epc_address_fields = duckdb.sql(sql)

sql = """
select
    unique_id,
    source_dataset,
    upper(concat(address1, ' ', address2, ' ', address3, ' ', POSTTOWN)) as address_concat,
    postcode
from df_epc_address_fields
"""

df_epc_fields_concat = duckdb.sql(sql)
df_epc_fields_concat.df()

sql = """
select
    unique_id,
    source_dataset,
    address_concat,
    postcode
from df_epc_fields_concat
union all
select

    unique_id,
    source_dataset,
    address_concat,
    postcode
from df_pp_fields_concat

"""
df_vertical_concat = duckdb.sql(sql)


sql = """
SELECT
    unique_id,
    source_dataset,
    -- Replace commas and dashes with spaces, remove apostrophes, and reduce multiple spaces to a single space
    trim(regexp_replace(

            regexp_replace(
                address_concat,
                 '[,\\-\u2013\u2014]', ' ', 'g'
            ),
            '[^a-zA-Z0-9 ]', '', 'g'  -- Remove anything that isn't a-zA-Z0-9 or space
        )

    ) AS address_concat_cleaned,
    address_concat,
    postcode
FROM df_vertical_concat
"""


df_all_concat_punc = duckdb.query(sql)


regex_pattern = r"\b\w*\d+\w*\b"


sql = f"""
SELECT
    unique_id,
    source_dataset,
    address_concat,
    regexp_replace(address_concat_cleaned, '{regex_pattern}', '', 'g') AS address_without_numbers,
    postcode,
    regexp_extract_all(address_concat_cleaned, '{regex_pattern}') AS numeric_tokens
FROM df_all_concat_punc
"""


df_all_numbers_in_array = duckdb.query(sql)


sql = f"""
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


sql = f"""
select
    unique_id,
    source_dataset,
    address_concat,
numeric_token_1,
numeric_token_2,
numeric_token_3,
regexp_split_to_array(trim(address_without_numbers), '\\s') as address_without_numbers_tokenised,
postcode
from df_all_numbers_as_cols
"""
df_all_numbers_as_cols_others_tokenised = duckdb.sql(sql)


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
from df_all_numbers_as_cols_others_tokenised
"""
df_all_numbers_as_cols_others_tokenised_distinct = duckdb.sql(sql)

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

final = duckdb.sql(sql)


# create splink_in/ folder if not exists

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
where source_dataset = 'epc'
) TO 'splink_in/epc.parquet' (FORMAT PARQUET);
"""
duckdb.sql(sql)

sql = """
select * from final
where unique_id in
('1096552219302014022515255225042148', '{53538A85-DAF9-436F-A770-3FE6AB56B9B9}')
"""
duckdb.sql(sql).df()
