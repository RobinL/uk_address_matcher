import duckdb

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

# File path to the CSV
file_path = "/Users/robinlinacre/Downloads/pp-complete.csv"

sql = f"""
    SELECT *
    FROM read_csv('{file_path}', header=False, columns={column_definitions})
"""
df_price_paid = duckdb.query(sql)


file_path_adur = "domestic-E07000223-Adur/certificates.csv"


df_epc_adur = duckdb.sql(
    f"""
    WITH ranked_entries AS (
        SELECT *, ROW_NUMBER() OVER(PARTITION BY uprn ORDER BY LODGEMENT_DATE DESC) as rn
        FROM read_csv_auto('{file_path_adur}')
    )
    SELECT * FROM ranked_entries WHERE rn = 1
    """
)


sql = """
select *
from df_price_paid
where df_price_paid.postcode in
(select postcode from df_epc_adur)
"""

price_paid_addresses = duckdb.sql(sql)


sql = """
COPY price_paid_addresses TO 'price_paid_addresses.parquet' (FORMAT PARQUET);
"""
duckdb.sql(sql)

sql = """
COPY df_epc_adur TO 'adur_epc.parquet' (FORMAT PARQUET);
"""
duckdb.sql(sql)
