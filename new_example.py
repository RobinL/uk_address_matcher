import duckdb
import pandas as pd
from IPython.display import display
import json
import importlib.resources as pkg_resources
from uk_address_matcher.cleaning_pipelines import (
    clean_data_using_precomputed_rel_tok_freq,
)

from splink import Linker, DuckDBAPI, SettingsCreator

pd.options.display.max_colwidth = 1000

# -----------------------------------------------------------------------------
# Step 1: Load in some example data.  If using your own data, it must be in»
# the same format as the example data.
# -----------------------------------------------------------------------------
p_fhrs = "example_data/fhrs_addresses_sample.parquet"
p_ch = "example_data/companies_house_addresess_postcode_overlap.parquet"

con = duckdb.connect(database=":memory:")
con.sql(f"CREATE TABLE df_fhrs AS SELECT * FROM read_parquet('{p_fhrs}')")
con.sql(f"CREATE TABLE df_ch AS SELECT * FROM read_parquet('{p_ch}')")
df_fhrs = con.table("df_fhrs")
df_ch = con.table("df_ch")


# Display length of the dataset
print(f"Length of FHRS dataset: {len(df_fhrs):,.0f}")
print(f"Length of Companies House dataset: {len(df_ch):,.0f}")

display(df_fhrs.limit(5).df())
display(df_ch.limit(5).df())
df_fhrs_clean = clean_data_using_precomputed_rel_tok_freq(df_fhrs, con=con)
df_ch_clean = clean_data_using_precomputed_rel_tok_freq(df_ch, con=con)
df_ch_clean


with (
    pkg_resources.files("uk_address_matcher.data")
    .joinpath("splink_model.json")
    .open("r") as f
):
    settings_as_dict = json.load(f)

settings = SettingsCreator.from_path_or_dict(settings_as_dict)

db_api = DuckDBAPI(con)
linker = Linker(
    [df_fhrs_clean, df_ch_clean],
    settings,
    db_api=db_api,
)

linker.visualisations.match_weights_chart()

df_predict = linker.inference.predict(experimental_optimisation=True)
