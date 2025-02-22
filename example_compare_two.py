import duckdb
import pandas as pd

from uk_address_matcher import clean_data_using_precomputed_rel_tok_freq, get_linker


con = duckdb.connect(database=":memory:")

dataset_1_dict = [
    {
        "unique_id": "1",
        "source_dataset": "dataset 1",
        "address_concat": "11A spitfire court 243 birmingham",
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
]
dataset_2 = pd.DataFrame(dataset_2_dict)
con.register("dataset_2", dataset_2)


cleaned_1 = clean_data_using_precomputed_rel_tok_freq(dataset_1, con=con)
cleaned_2 = clean_data_using_precomputed_rel_tok_freq(dataset_2, con=con)

linker = get_linker(
    df_addresses_to_match=cleaned_1,
    df_addresses_to_search_within=cleaned_2,
    con=con,
    include_full_postcode_block=True,
    additional_columns_to_retain=["original_address_concat"],
)


res = linker.inference.compare_two_records(
    record_1=cleaned_1, record_2=cleaned_2, include_found_by_blocking_rules=True
)

res_ddb = res.as_duckdbpyrelation()
res_ddb.show(max_width=400)

recs = res_ddb.df().to_dict(orient="records")
linker.visualisations.waterfall_chart(recs)
