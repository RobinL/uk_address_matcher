import duckdb
import pandas as pd
from IPython.display import display

from uk_address_matcher.cleaning_pipelines import (
    clean_data_using_precomputed_rel_tok_freq,
)
from uk_address_matcher.display_results import display_l_r, display_token_rel_freq
from uk_address_matcher.splink_model import get_pretrained_linker

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
    {
        "unique_id": "4",
        "source_dataset": "dataset 2",
        "address_concat": "flat A, 11 243 high street birmingham",
        "postcode": "B12 1CD",
    },
]

dataset_2 = pd.DataFrame(dataset_2_dict)
con.register("dataset_2", dataset_2)


cleaned_1 = clean_data_using_precomputed_rel_tok_freq(dataset_1, con=con)
cleaned_2 = clean_data_using_precomputed_rel_tok_freq(dataset_2, con=con)


linker = get_pretrained_linker([cleaned_1, cleaned_2], con=con)


df_predict = linker.predict()
df_predict_pd = df_predict.as_pandas_dataframe()
df_predict_pd = df_predict_pd.sort_values("match_probability", ascending=False)
df_predict_pd
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

    display(display_l_r(row_as_df))

    display(display_token_rel_freq(row_as_df, "token_rel_freq_arr_l"))
    display(display_token_rel_freq(row_as_df, "token_rel_freq_arr_r"))

    row_dict = row.to_dict()
    display(linker.waterfall_chart([row_dict]))
