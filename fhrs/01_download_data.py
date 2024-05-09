import duckdb
import pandas as pd
import requests
from lxml import etree

pd.options.display.max_columns = None
pd.options.display.max_colwidth = None


# --------------------------------
# Load the price paid data
# --------------------------------

# Define the column names and types based on the description provided
column_definitions = {
    "transaction_unique_identifier": "VARCHAR",
    "price": "BIGINT",
    "date_of_transfer": "DATE",
    "postcode": "VARCHAR",
    "property_type": "VARCHAR(1)",
    "old_new": "VARCHAR(1)",
    "duration": "VARCHAR(1)",
    "paon": "VARCHAR",
    "saon": "VARCHAR",
    "street": "VARCHAR",
    "locality": "VARCHAR",
    "town_city": "VARCHAR",
    "district": "VARCHAR",
    "county": "VARCHAR",
    "ppd_category_type": "VARCHAR(1)",
    "record_status": "VARCHAR(1)",
}


file_path = "/Users/robinlinacre/Downloads/pp-complete.csv"

sql = f"""
    SELECT *
    FROM read_csv('{file_path}', header=False, columns={column_definitions})
"""
df_price_paid = duckdb.query(sql)

# FHRS data


def xml_to_dataframe(xml_url):
    response = requests.get(xml_url)
    tree = etree.fromstring(response.content)

    data = []
    for element in tree.findall(".//EstablishmentDetail"):
        record = {}
        for child in element:
            record[child.tag] = child.text
        data.append(record)

    return pd.DataFrame(data)


urls = pd.read_html("https://ratings.food.gov.uk/open-data", extract_links="all")[
    0
].iloc[:, 0]
xml_urls = ["https://ratings.food.gov.uk/" + u[1] for u in list(urls)]

fhrs_dfs = []

for xml_url in xml_urls:
    print(xml_url)

    fhrs_df = xml_to_dataframe(xml_url)
    f1 = fhrs_df["RatingValue"] != "Exempt"
    address_cols = [
        "FHRSID",
        "AddressLine1",
        "AddressLine2",
        "AddressLine3",
        "AddressLine4",
        "PostCode",
    ]

    # f1 = df["FHRSID"]  == '1468881'
    # display(df[f1])

    for col in [
        "AddressLine1",
        "AddressLine2",
        "AddressLine3",
        "AddressLine4",
        "PostCode",
    ]:
        if col not in fhrs_df.columns:
            fhrs_df[col] = None

    fhrs_df_filtered = fhrs_df[f1][address_cols]

    # Replace NaN with true string null
    fhrs_df_filtered = fhrs_df_filtered.astype(pd.StringDtype())

    fhrs_df_filtered.columns = fhrs_df_filtered.columns.str.lower()
    fhrs_dfs.append(fhrs_df_filtered)

all_fhrs_data = pd.concat(fhrs_dfs)


all_fhrs_data = all_fhrs_data.dropna(
    how="all", subset=all_fhrs_data.columns.str.lower()[1:]
)

all_fhrs_data = all_fhrs_data.dropna(how="all", subset="postcode")


all_fhrs_data.to_parquet("raw_data/fhrs_data.parquet")


sql = """
select *
from df_price_paid
where df_price_paid.postcode in
(select postcode from all_fhrs_data)
"""

price_paid_addresses = duckdb.sql(sql)

sql = """
COPY price_paid_addresses TO 'raw_data/price_paid_addresses.parquet' (FORMAT PARQUET);
"""
duckdb.sql(sql)
