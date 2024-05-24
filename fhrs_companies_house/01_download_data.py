# https://download.companieshouse.gov.uk/en_output.html
# https://download.companieshouse.gov.uk/BasicCompanyDataAsOneFile-2024-05-01.zip

import duckdb
import pandas as pd

pd.options.display.max_colwidth = 1000

# sql = """
# select *
# from read_csv_auto('./BasicCompanyDataAsOneFile-2024-05-01.csv')
# limit 10

# """


# sql = """
# SELECT
#     "CompanyName" AS company_name,
#     "CompanyNumber" AS company_number,
#     "RegAddress.AddressLine1" AS address_line1,
#     "RegAddress.AddressLine2" AS address_line2,
#     "RegAddress.PostTown" AS post_town,
#     "RegAddress.County" AS county,
#     "RegAddress.Country" AS country,
#     "RegAddress.PostCode" AS post_code
# FROM
#     read_csv_auto('./BasicCompanyDataAsOneFile-2024-05-01.csv')
# LIMIT 10
# """

# result = duckdb.sql(sql).df()


sql = """
SELECT
    "CompanyNumber" AS unique_id,
    'companies_house' as source_dataset,
    CONCAT(
        COALESCE("RegAddress.AddressLine1", ''), ' ',
        COALESCE("RegAddress.AddressLine2", ''), ' ',
        COALESCE("RegAddress.PostTown", ''), ' ',
        COALESCE("RegAddress.County", '')
    ) AS address_concat,
    "RegAddress.PostCode" AS postcode
FROM
    read_csv_auto('./BasicCompanyDataAsOneFile-2024-05-01.csv')

"""

companies_house_addresess = duckdb.sql(sql)


df_fhrs = pd.read_parquet("../fhrs/raw_data/fhrs_data.parquet")

sql = """
select
    fhrsid as unique_id,
    'fhrs' as source_dataset,
    upper(concat(addressline1, ' ', addressline2, ' ', addressline3, ' ', addressline4)) as address_concat,
    postcode

from df_fhrs


"""
fhrs_addresses = duckdb.sql(sql)


sql = """
select *
from companies_house_addresess
where postcode in
(select postcode from fhrs_addresses)
"""
