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
from uk_address_matcher.cleaning_pipelines import (
    clean_data_using_precomputed_rel_tok_freq,
)
from uk_address_matcher.splink_model import _performance_predict

df_1_c = clean_data_using_precomputed_rel_tok_freq(df_1, con=con)
df_2_c = clean_data_using_precomputed_rel_tok_freq(df_2, con=con)


linker, predictions = _performance_predict(
    df_addresses_to_match=df_1_c,
    df_addresses_to_search_within=df_2_c,
    con=con,
    match_weight_threshold=-10,
    output_all_cols=True,
    include_full_postcode_block=True,
)
```

Initial tests suggest you can match ~ 1,000 addresses per second against a list of 30 million addresses on a laptop.

Refer to [the example](example.py), which has detailed comments, for how to match your data.

See [an example of comparing two addresses](example_compare_two.py) to get a sense of what it does/how it scores

Run an interactive example in your browser:

[![Open In Colab](https://colab.research.google.com/assets/colab-badge.svg)](https://colab.research.google.com/github/RobinL/uk_address_matcher/blob/main/match_example_data.ipynb)  Match 5,000 FHRS records to 21,952 companies house records in < 10 seconds.

[![Open In Colab](https://colab.research.google.com/assets/colab-badge.svg)](https://colab.research.google.com/github/RobinL/uk_address_matcher/blob/main/interactive_comparison.ipynb) Investigate and understand how the model works



