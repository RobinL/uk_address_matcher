import duckdb
import pandas as pd
from IPython.display import display

from address_matching.cleaning_pipelines import (
    clean_data_using_precomputed_rel_tok_freq,
)
from address_matching.display_results import display_columns, display_token_rel_freq
from address_matching.splink_model import get_pretrained_linker

con = duckdb.connect(database=":memory:")

dataset_1_dict = [
    {
        "unique_id": "1",
        "source_dataset": "dataset 1",
        "address_concat": "flat 11A 243 high street birmingham",
        "postcode": "B12 0AB",
    },
]
dataset_1 = pd.DataFrame(dataset_1_dict)
con.register("dataset_1", dataset_1)


dataset_2_dict = [
    {
        "unique_id": "2",
        "source_dataset": "dataset 2",
        "address_concat": "flat A, 11 spitfire court 243 high street birmingham",
        "postcode": "B12 0AB",
    },
    {
        "unique_id": "3",
        "source_dataset": "dataset 2",
        "address_concat": "11 spitfire court 243 high street birmingham",
        "postcode": "B12 0AB",
    },
]

dataset_2 = pd.DataFrame(dataset_2_dict)
con.register("dataset_2", dataset_2)

path = "./example_data/rel_tok_freq.parquet"
rel_tok_freq = con.sql(f"SELECT token, rel_freq FROM read_parquet('{path}')")


cleaned_1 = clean_data_using_precomputed_rel_tok_freq(
    dataset_1, rel_tok_freq_table=rel_tok_freq, con=con
)
cleaned_2 = clean_data_using_precomputed_rel_tok_freq(
    dataset_2, rel_tok_freq_table=rel_tok_freq, con=con
)


path = "./example_data/numeric_token_tf_table.parquet"
sql = f" SELECT * FROM read_parquet('{path}')"
numeric_token_freq = con.sql(sql)

linker = get_pretrained_linker(
    [cleaned_1, cleaned_2], precomputed_numeric_tf_table=numeric_token_freq, con=con
)

df_predict = linker.predict()
df_predict_pd = df_predict.as_pandas_dataframe()
df_predict_pd = df_predict_pd.sort_values("match_probability", ascending=False)

sql_expr = (
    linker._settings_obj.comparisons[4]
    .comparison_levels[2]
    .sql_condition.replace(" < 1e-18", "")
)

sql = f"""
select {sql_expr} as token_rel_product,
array_transform(token_rel_freq_arr_l, x -> x.tok) as toks_l,
array_transform(token_rel_freq_arr_r, x -> x.tok) as toks_r,
original_address_concat_l as ad_l,
original_address_concat_r as ad_r

from {df_predict.physical_name}
"""
linker.query_sql(sql)

# Assuming df_predict_pd is your DataFrame and linker is your object with waterfall_chart method
for index, row in df_predict_pd.iterrows():
    row_as_df = pd.DataFrame([row])

    display(display_columns(row_as_df, "_l"))
    display(display_columns(row_as_df, "_r"))

    display(display_token_rel_freq(row_as_df, "token_rel_freq_arr_l"))
    display(display_token_rel_freq(row_as_df, "token_rel_freq_arr_r"))

    row_dict = row.to_dict()
    display(linker.waterfall_chart([row_dict]))
