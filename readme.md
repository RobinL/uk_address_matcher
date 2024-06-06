# A first stab at address matching using Splink

This repo contains generic code for address matching using Splink. At the moment it assumes you block on postcode.

It's assumed you have two input files of addresses with the following columns:

- `unique_id`
- `address_concat`
- `postcode`

Refer to [the example](example.py) for how to match your data.

There are also some folders (epc, fhrs, fhrs_companies_house) with some example code of how to collect additional data. Please ensure you read and respect the licence of the respective datasets if you use them.

This is very much work in progress!

## Step by step

In the [the example](example.py) we use a subset of data from fhrs and companies house, in total around 27,000 rows.

This step by step guide annotates the code in [the example](example.py).

## Step 1: Concat

We first concat the two datasets:

```
sql = f"""
select *, address_concat as original_address_concat
from read_parquet('./example_data/companies_house_addresess_postcode_overlap.parquet')
UNION ALL
select *, address_concat as original_address_concat
from read_parquet('./example_data/fhrs_addresses_sample.parquet')
"""
df_unioned = duckdb.sql(sql)
```

The reason for this is because we're going to want to gather term frequcnies for each of the tokens in the address, and we want this to be based on the full dataset

## Step 1: Clean

Next, we use `address_matching.cleaning` functions in a pipeline to clean up and standarise the data:

```python
cleaning_queue = [
    trim_whitespace_address_and_postcode,
    upper_case_address_and_postcode,
    clean_address_string_first_pass,
    parse_out_numbers,
    clean_address_string_second_pass,
    split_numeric_tokens_to_cols,
    tokenise_address_without_numbers,
    add_term_frequencies_to_address_tokens,
    move_common_end_tokens_to_field,
    first_unusual_token,
    use_first_unusual_token_if_no_numeric_token,
    final_column_order,
]


df_cleaned = run_pipeline(df_unioned, cleaning_queue, print_intermediate=False)
```

`run_pipeline` runs each function in a big CTE SQL query, so it's quite efficient.

# Step 2: Split back into two dataset to put into splink

```python
df_1 = df_cleaned.filter("source_dataset == 'companies_house'").df()
df_2 = df_cleaned.filter("source_dataset == 'fhrs'").df()
```

# Step 3: Train model

```python
linker = train_splink_model(df_1, df_2)
```

# Step 4: Investigate results

```python
linker = train_splink_model(df_1, df_2)

df_predict = linker.predict(threshold_match_probability=0.5)

sql = f"""
select
    unique_id_l,
    unique_id_r,
    source_dataset_l,
    source_dataset_r,
    match_probability,
    match_weight,
    original_address_concat_l,
    original_address_concat_r
    from {df_predict.physical_name}
where match_weight > 0.5
order by random()
limit 10
"""

res = linker.query_sql(sql)
res

sql = f"""
select * from {df_predict.physical_name}
where match_weight > 0.5
order by random()
limit 3
"""
recs = linker.query_sql(sql).to_dict(orient="records")


for rec in recs:
    print("-" * 80)
    print(rec["unique_id_l"], rec["original_address_concat_l"])
    print(rec["unique_id_r"], rec["original_address_concat_r"])
    display(linker.waterfall_chart([rec]))

```

#Some other notes:

## Feature engineering

I've had a rough stab at feature engineering in the cleaning functions

Due to the [unusual nature of addresses](https://www.mjt.me.uk/posts/falsehoods-programmers-believe-about-addresses/) this pre-processing is a lot more sophisticated than would be done with many other entity types such as peoples' names.

Most of the decisions are fairly arbitrary, and were arrived at by experimentation - an iterative cycle of feature engineering, model training, and inspecting what the model was getting wrong.

## Model training

Splink is used to train a model. I found that training the m values did more harm than good, so I've only bothered to train the u values.

From inspecting some results, this model does a fairly decent job. Where we see errors, it's seems to be mostly because the true address does not exist. This would happen when a property has an EPC but is not in the price paid data.

But I haven't looked in any depth so your mileage may vary.

## Match weights and dasbhboard

See [comparison_viewer_dashboard](comparison_viewer_dashboard.html) and [match_weights](match_weights.html)

![image](https://github.com/RobinL/address_matching_example/assets/2608005/7f025849-c7e2-4687-b0ae-8cabbe3977b9)
