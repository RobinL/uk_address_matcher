import duckdb
import pandas as pd
import requests
from lxml import etree

OUT_FOLDER = "./secret_data/fhrs/"


def xml_to_dataframe(xml_url):
    response = requests.get(xml_url)
    tree = etree.fromstring(response.content)

    data = []
    for element in tree.findall(".//EstablishmentDetail"):
        record = {}
        for child in element:
            if child.tag == "Geocode":
                longitude = child.find("Longitude")
                latitude = child.find("Latitude")
                if longitude is not None:
                    record["geocode_longitude"] = longitude.text
                if latitude is not None:
                    record["geocode_latitude"] = latitude.text
            else:
                record[child.tag] = child.text
        data.append(record)

    return pd.DataFrame(data)


all_urls = []
html_data = pd.read_html("https://ratings.food.gov.uk/open-data", extract_links="all")
for d in html_data:
    urls = d.iloc[:, 0]

    all_urls.extend(urls)


print("Downloading Medway only, remove the if statement to download all")
xml_urls = [
    "https://ratings.food.gov.uk/" + u[1] for u in list(all_urls) if "Medway" in u[0]
]
xml_urls = ["https://ratings.food.gov.uk/" + u[1] for u in list(all_urls)]


fhrs_dfs = []

for xml_url in xml_urls:
    print(xml_url)

    fhrs_df = xml_to_dataframe(xml_url)
    f1 = fhrs_df["RatingValue"] != "Exempt"
    address_cols = [
        "FHRSID",
        "BusinessName",
        "AddressLine1",
        "AddressLine2",
        "AddressLine3",
        "AddressLine4",
        "PostCode",
        "geocode_longitude",
        "geocode_latitude",
    ]

    for col in [
        "BusinessName",
        "AddressLine1",
        "AddressLine2",
        "AddressLine3",
        "AddressLine4",
        "PostCode",
        "geocode_longitude",
        "geocode_latitude",
    ]:
        if col not in fhrs_df.columns:
            fhrs_df[col] = None

    fhrs_df_filtered = fhrs_df[f1][address_cols]

    string_columns = [
        "FHRSID",
        "BusinessName",
        "AddressLine1",
        "AddressLine2",
        "AddressLine3",
        "AddressLine4",
        "PostCode",
    ]
    fhrs_df_filtered[string_columns] = (
        fhrs_df_filtered[string_columns].astype("string").fillna("")
    )

    fhrs_df_filtered = fhrs_df_filtered.rename(
        columns={"geocode_longitude": "lng", "geocode_latitude": "lat"}
    )

    numeric_columns = ["lng", "lat"]
    fhrs_df_filtered[numeric_columns] = fhrs_df_filtered[numeric_columns].astype(
        "float64"
    )

    fhrs_df_filtered.columns = fhrs_df_filtered.columns.str.lower()
    fhrs_dfs.append(fhrs_df_filtered)

all_fhrs_data = pd.concat(fhrs_dfs)


sql = """

select
nullif(fhrsid, '') as fhrsid,
nullif(businessname, '') as businessname,
nullif(addressline1, '') as addressline1,
nullif(addressline2, '') as addressline2,
nullif(addressline3, '') as addressline3,
nullif(addressline4, '') as addressline4,
nullif(postcode, '') as postcode,
lng,
lat
from all_fhrs_data
where not nullif(addressline1, '') is null and not nullif(postcode, '') is null
"""
cleaned = duckdb.sql(sql)
cleaned.show(max_width=5000)

cleaned.to_parquet(f"{OUT_FOLDER}/fhrs_data.parquet")


lat_percentage_query = """
SELECT
    COUNT(*) AS total_rows,
    COUNT(lat) AS rows_with_lat,
    (COUNT(lat) * 100.0 / COUNT(*)) AS percentage_with_lat,

FROM cleaned
"""

lat_stats = duckdb.sql(lat_percentage_query)
lat_stats.show()
