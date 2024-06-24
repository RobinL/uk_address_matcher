import duckdb
import pandas as pd
from IPython.display import display

from address_matching.cleaning_pipelines import (
    clean_data_using_precomputed_rel_tok_freq,
)
from address_matching.splink_model import get_pretrained_linker


def transform_dataframe(df):
    # List of columns that end with _l or _r
    l_cols = [col for col in df.columns if col.endswith("_l")]
    r_cols = [col for col in df.columns if col.endswith("_r")]

    # List of columns that don't end with _l or _r
    common_cols = [
        col for col in df.columns if not col.endswith("_l") and not col.endswith("_r")
    ]

    # Filter out columns starting with 'gamma' or 'bf'
    l_cols = [
        col
        for col in l_cols
        if not col.startswith("gamma") and not col.startswith("bf")
    ]
    r_cols = [
        col
        for col in r_cols
        if not col.startswith("gamma") and not col.startswith("bf")
    ]
    common_cols = [
        col
        for col in common_cols
        if not col.startswith("gamma") and not col.startswith("bf")
    ]

    # Extract common columns and duplicated values
    common_df = df[common_cols].copy()
    common_df_l = common_df.copy()
    common_df_r = common_df.copy()

    # Transform _l columns
    df_l = df[l_cols].copy()
    df_l.columns = [col[:-2] for col in df_l.columns]
    df_l = pd.concat([common_df_l, df_l], axis=1)

    # Transform _r columns
    df_r = df[r_cols].copy()
    df_r.columns = [col[:-2] for col in df_r.columns]
    df_r = pd.concat([common_df_r, df_r], axis=1)

    # Concatenate the transformed DataFrames
    result_df = pd.concat([df_l, df_r], axis=0, ignore_index=True)

    return result_df


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


for index, row in df_predict_pd.iterrows():

    row_as_df = pd.DataFrame([row])
    cols = [row_as_df.column]

    cols_ending_in_l = [
        col
        for col in cols
        if col.endswith("_l")
        and not col.startswith("bf")
        and not col.startswith("gamma")
    ]
    cols_ending_in_r = [
        col
        for col in cols
        if col.endswith("_r")
        and not col.startswith("bf")
        and not col.startswith("gamma")
    ]
    display(row_as_df[cols_ending_in_l])
    display(row_as_df[cols_ending_in_r])
    row_dict = row.to_dict()

    display(linker.waterfall_chart([row_dict]))
