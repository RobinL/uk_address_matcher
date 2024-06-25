import duckdb
import pandas as pd
from IPython.display import display

from uk_address_matcher.cleaning_pipelines import (
    clean_data_using_precomputed_rel_tok_freq,
)
from uk_address_matcher.display_results import display_l_r, display_token_rel_freq
from uk_address_matcher.splink_model import _performance_predict

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

"11 spitfire court 243 high street birmingham B12 0AB",
"flat A, 11 243 high street birmingham B12 1CD",
dataset_2_dict = [
    {
        "unique_id": "2",
        "source_dataset": "dataset 2",
        "address_concat": "flat A, 11 spitfire court 243 high street birmingham",
        "postcode": "B12 0AB",
    },
]

dataset_2 = pd.DataFrame(dataset_2_dict)
con.register("dataset_2", dataset_2)


cleaned_1 = clean_data_using_precomputed_rel_tok_freq(dataset_1, con=con)
cleaned_2 = clean_data_using_precomputed_rel_tok_freq(dataset_2, con=con)

linker, predictions = _performance_predict(
    [cleaned_1, cleaned_2],
    con=con,
    match_weight_threshold=-10,
    output_all_cols=True,
    include_full_postcode_block=True,
)

recs = predictions.df().to_dict(orient="records")
recs
display(linker.waterfall_chart(recs))
