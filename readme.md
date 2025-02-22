# Matching UK addresses using Splink

High performance address matching using a pre-trained [Splink](https://github.com/moj-analytical-services/splink) model.

Assuming you have two duckdb dataframes  in this format:

| unique_id | address_concat               | postcode  |
|-----------|------------------------------|-----------|
| 1         | 123 Fake Street, Faketown    | FA1 2KE   |
| 2         | 456 Other Road, Otherville   | NO1 3WY   |
| ...       | ...                          | ...       |


Match them with:

```python
import duckdb

from uk_address_matcher import clean_data_using_precomputed_rel_tok_freq, get_linker

p_ch = "./example_data/companies_house_addresess_postcode_overlap.parquet"
p_fhrs = "./example_data/fhrs_addresses_sample.parquet"

con = duckdb.connect(database=":memory:")

df_ch = con.read_parquet(p_ch).order("postcode")
df_fhrs = con.read_parquet(p_fhrs).order("postcode")

df_ch_clean = clean_data_using_precomputed_rel_tok_freq(df_ch, con=con)
df_fhrs_clean = clean_data_using_precomputed_rel_tok_freq(df_fhrs, con=con)

linker = get_linker(
    df_addresses_to_match=df_fhrs_clean,
    df_addresses_to_search_within=df_ch_clean,
    con=con,
    include_full_postcode_block=True,
    additional_columns_to_retain=["original_address_concat"],
)

df_predict = linker.inference.predict(
    threshold_match_weight=-50, experimental_optimisation=True
)
df_predict_ddb = df_predict.as_duckdbpyrelation()
```

Initial tests suggest you can match ~ 1,000 addresses per second against a list of 30 million addresses on a laptop.

Refer to [the example](example_matching.py), which has detailed comments, for how to match your data.

See [an example of comparing two addresses](example_compare_two.py) to get a sense of what it does/how it scores

Run an interactive example in your browser:

[![Open In Colab](https://colab.research.google.com/assets/colab-badge.svg)](https://colab.research.google.com/github/RobinL/uk_address_matcher/blob/main/match_example_data.ipynb)  Match 5,000 FHRS records to 21,952 companies house records in < 10 seconds.

[![Open In Colab](https://colab.research.google.com/assets/colab-badge.svg)](https://colab.research.google.com/github/RobinL/uk_address_matcher/blob/main/interactive_comparison.ipynb) Investigate and understand how the model works



